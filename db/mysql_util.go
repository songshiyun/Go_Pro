package db

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

const digits01 = "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789"
const digits10 = "0000000000111111111122222222223333333333444444444455555555556666666666777777777788888888889999999999"

func ParsePlaceHolderSqlToRaws(query string, args []interface{}) (string, error) {
	namedArgs, err := driverArgsConnLocked(args)
	if err != nil {
		return "", err
	}
	values, err := namedValueToValue(namedArgs)
	if err != nil {
		return "", err
	}
	return interpolateParams(query, values)
}

func interpolateParams(query string, args []driver.Value) (string, error) {
	if strings.Count(query, "?") != len(args) {
		return "", fmt.Errorf("sql: expected %d arguments, got %d", strings.Count(query, "?"), len(args))
	}
	var buf []byte
	buf = buf[:0]
	argPos := 0
	for i := 0; i < len(query); i++ {
		q := strings.IndexByte(query[i:], '?')
		if q == -1 {
			buf = append(buf, query[i:]...)
			break
		}
		buf = append(buf, query[i:i+q]...)
		i += q

		arg := args[argPos]
		argPos++

		if arg == nil {
			buf = append(buf, "NULL"...)
			continue
		}

		switch v := arg.(type) {
		case int64:
			buf = strconv.AppendInt(buf, v, 10)
		case uint64:
			// Handle uint64 explicitly because our custom ConvertValue emits unsigned values
			buf = strconv.AppendUint(buf, v, 10)
		case float64:
			buf = strconv.AppendFloat(buf, v, 'g', -1, 64)
		case bool:
			if v {
				buf = append(buf, '1')
			} else {
				buf = append(buf, '0')
			}
		case time.Time:
			if v.IsZero() {
				buf = append(buf, "'0000-00-00'"...)
			} else {
				loc, _ := time.LoadLocation("Asia/Shanghai")
				v := v.In(loc)
				v = v.Add(time.Nanosecond * 500) // To round under microsecond
				year := v.Year()
				year100 := year / 100
				year1 := year % 100
				month := v.Month()
				day := v.Day()
				hour := v.Hour()
				minute := v.Minute()
				second := v.Second()
				micro := v.Nanosecond() / 1000

				buf = append(buf, []byte{
					'\'',
					digits10[year100], digits01[year100],
					digits10[year1], digits01[year1],
					'-',
					digits10[month], digits01[month],
					'-',
					digits10[day], digits01[day],
					' ',
					digits10[hour], digits01[hour],
					':',
					digits10[minute], digits01[minute],
					':',
					digits10[second], digits01[second],
				}...)

				if micro != 0 {
					micro10000 := micro / 10000
					micro100 := micro / 100 % 100
					micro1 := micro % 100
					buf = append(buf, []byte{
						'.',
						digits10[micro10000], digits01[micro10000],
						digits10[micro100], digits01[micro100],
						digits10[micro1], digits01[micro1],
					}...)
				}
				buf = append(buf, '\'')
			}
		case []byte:
			if v == nil {
				buf = append(buf, "NULL"...)
			} else {
				buf = append(buf, "_binary'"...)
				buf = escapeBytesQuotes(buf, v)
				buf = append(buf, '\'')
			}
		case string:
			buf = append(buf, '\'')
			buf = escapeStringQuotes(buf, v)
			buf = append(buf, '\'')
		default:
			return "", driver.ErrSkip
		}
	}
	if argPos != len(args) {
		return "", driver.ErrSkip
	}
	return string(buf), nil
}

// escapeStringQuotes is similar to escapeBytesQuotes but for string.
func escapeStringQuotes(buf []byte, v string) []byte {
	pos := len(buf)
	buf = reserveBuffer(buf, len(v)*2)

	for i := 0; i < len(v); i++ {
		c := v[i]
		if c == '\'' {
			buf[pos] = '\''
			buf[pos+1] = '\''
			pos += 2
		} else {
			buf[pos] = c
			pos++
		}
	}

	return buf[:pos]
}

// escapeBytesQuotes escapes apostrophes in []byte by doubling them up.
// This escapes the contents of a string by doubling up any apostrophes that
// it contains. This is used when the NO_BACKSLASH_ESCAPES SQL_MODE is in
// effect on the server.
// https://github.com/mysql/mysql-server/blob/mysql-5.7.5/mysys/charset.c#L963-L1038
func escapeBytesQuotes(buf, v []byte) []byte {
	pos := len(buf)
	buf = reserveBuffer(buf, len(v)*2)

	for _, c := range v {
		if c == '\'' {
			buf[pos] = '\''
			buf[pos+1] = '\''
			pos += 2
		} else {
			buf[pos] = c
			pos++
		}
	}

	return buf[:pos]
}

// reserveBuffer checks cap(buf) and expand buffer to len(buf) + appendSize.
// If cap(buf) is not enough, reallocate new buffer.
func reserveBuffer(buf []byte, appendSize int) []byte {
	newSize := len(buf) + appendSize
	if cap(buf) < newSize {
		// Grow buffer exponentially
		newBuf := make([]byte, len(buf)*2+appendSize)
		copy(newBuf, buf)
		buf = newBuf
	}
	return buf[:newSize]
}

// escapeBytesBackslash escapes []byte with backslashes (\)
// This escapes the contents of a string (provided as []byte) by adding backslashes before special
// characters, and turning others into specific escape sequences, such as
// turning newlines into \n and null bytes into \0.
// https://github.com/mysql/mysql-server/blob/mysql-5.7.5/mysys/charset.c#L823-L932
func escapeBytesBackslash(buf, v []byte) []byte {
	pos := len(buf)
	buf = reserveBuffer(buf, len(v)*2)

	for _, c := range v {
		switch c {
		case '\x00':
			buf[pos] = '\\'
			buf[pos+1] = '0'
			pos += 2
		case '\n':
			buf[pos] = '\\'
			buf[pos+1] = 'n'
			pos += 2
		case '\r':
			buf[pos] = '\\'
			buf[pos+1] = 'r'
			pos += 2
		case '\x1a':
			buf[pos] = '\\'
			buf[pos+1] = 'Z'
			pos += 2
		case '\'':
			buf[pos] = '\\'
			buf[pos+1] = '\''
			pos += 2
		case '"':
			buf[pos] = '\\'
			buf[pos+1] = '"'
			pos += 2
		case '\\':
			buf[pos] = '\\'
			buf[pos+1] = '\\'
			pos += 2
		default:
			buf[pos] = c
			pos++
		}
	}

	return buf[:pos]
}

func driverArgsConnLocked(args []interface{}) ([]driver.NamedValue, error) {
	nargs := make([]driver.NamedValue, len(args))
	var err error
	var n int
	for _, arg := range args {
		nv := &nargs[n]
		if np, ok := arg.(sql.NamedArg); ok {
			if err = validateNamedValueName(np.Name); err != nil {
				return nil, err
			}
			arg = np.Value
			nv.Name = np.Name
		}
		nv.Ordinal = n + 1
		nv.Value = arg
		checker := defaultCheckNamedValue
		//nextCC := false
	nextCheck:
		err = checker(nv)
		switch err {
		case nil:
			n++
			continue
		case driver.ErrRemoveArgument:
			nargs = nargs[:len(nargs)-1]
			continue
		case driver.ErrSkip:
			checker = defaultCheckNamedValue
			goto nextCheck
		default:
			return nil, fmt.Errorf("sql: converting argument %s type: %v", describeNamedValue(nv), err)
		}
	}
	return nargs, nil
}

type ccon struct{}

func (cc *conn) CheckNamedValue(nv *driver.NamedValue) (err error) {
	nv.Value, err = converter{}.ConvertValue(nv.Value)
	return
}

type converter struct{}

func (c converter) ConvertValue(v interface{}) (driver.Value, error) {
	if driver.IsValue(v) {
		return v, nil
	}
	if vr, ok := v.(driver.Valuer); ok {
		sv, err := callValuerValue(vr)
		if err != nil {
			return nil, err
		}
		if !driver.IsValue(sv) {
			return nil, fmt.Errorf("non-Value type %T returned from Value", sv)
		}
		return sv, nil
	}

	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Ptr:
		// indirect pointers
		if rv.IsNil() {
			return nil, nil
		} else {
			return c.ConvertValue(rv.Elem().Interface())
		}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return rv.Int(), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return rv.Uint(), nil
	case reflect.Float32, reflect.Float64:
		return rv.Float(), nil
	case reflect.Bool:
		return rv.Bool(), nil
	case reflect.Slice:
		ek := rv.Type().Elem().Kind()
		if ek == reflect.Uint8 {
			return rv.Bytes(), nil
		}
		return nil, fmt.Errorf("unsupported type %T, a slice of %s", v, ek)
	case reflect.String:
		return rv.String(), nil
	}
	return nil, fmt.Errorf("unsupported type %T, a %s", v, rv.Kind())
}

func defaultCheckNamedValue(nv *driver.NamedValue) (err error) {
	nv.Value, err = driver.DefaultParameterConverter.ConvertValue(nv.Value)
	return err
}

var valuerReflectType = reflect.TypeOf((*driver.Valuer)(nil)).Elem()

func callValuerValue(vr driver.Valuer) (v driver.Value, err error) {
	if rv := reflect.ValueOf(vr); rv.Kind() == reflect.Ptr &&
		rv.IsNil() &&
		rv.Type().Elem().Implements(valuerReflectType) {
		return nil, nil
	}
	return vr.Value()
}

func namedValueToValue(named []driver.NamedValue) ([]driver.Value, error) {
	dargs := make([]driver.Value, len(named))
	for n, param := range named {
		if len(param.Name) > 0 {
			return nil, errors.New("sql: driver does not support the use of Named Parameters")
		}
		dargs[n] = param.Value
	}
	return dargs, nil
}

func validateNamedValueName(name string) error {
	if len(name) == 0 {
		return nil
	}
	r, _ := utf8.DecodeRuneInString(name)
	if unicode.IsLetter(r) {
		return nil
	}
	return fmt.Errorf("name %q does not begin with a letter", name)
}

func describeNamedValue(nv *driver.NamedValue) string {
	if len(nv.Name) == 0 {
		return fmt.Sprintf("$%d", nv.Ordinal)
	}
	return fmt.Sprintf("with name %q", nv.Name)
}

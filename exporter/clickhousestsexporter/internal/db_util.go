package internal

import (
	"fmt"
	"time"
)

func GenerateTTLExpr(ttlDays uint, ttl time.Duration, timeField string) string {
	if ttlDays > 0 {
		return fmt.Sprintf(`TTL toDateTime(%s) + toIntervalDay(%d)`, timeField, ttlDays)
	}

	if ttl > 0 {
		switch {
		case ttl%(24*time.Hour) == 0:
			return fmt.Sprintf(`TTL toDateTime(%s) + toIntervalDay(%d)`, timeField, ttl/(24*time.Hour))
		case ttl%(time.Hour) == 0:
			return fmt.Sprintf(`TTL toDateTime(%s) + toIntervalHour(%d)`, timeField, ttl/time.Hour)
		case ttl%(time.Minute) == 0:
			return fmt.Sprintf(`TTL toDateTime(%s) + toIntervalMinute(%d)`, timeField, ttl/time.Minute)
		default:
			return fmt.Sprintf(`TTL toDateTime(%s) + toIntervalSecond(%d)`, timeField, ttl/time.Second)
		}
	}
	return ""
}

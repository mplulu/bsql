package bsql

import "time"

func ParseTimestampTZ(timeStr string) (time.Time, error) {
	// 2020-07-06 17:35:04+07
	layout := "2006-01-02 15:04:05-07"
	return time.Parse(layout, timeStr)
}

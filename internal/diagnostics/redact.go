package diagnostics

import (
	"regexp"
)

type Redactor struct {
	patterns []redactionPattern
}

type redactionPattern struct {
	expr        *regexp.Regexp
	replacement string
}

func NewRedactor() Redactor {
	return Redactor{
		patterns: []redactionPattern{
			mustPattern(`(?i)bearer\s+[A-Za-z0-9._~+/=-]+`, "Bearer [REDACTED]"),
			mustPattern(`(?i)(token|secret|api[_-]?key|password)=([^ \n\r\t]+)`, "$1=[REDACTED]"),
			mustPattern(`(?i)(prompt|fully_composed_prompt)=([^ \n\r\t]+)`, "$1=[REDACTED]"),
			mustPattern(`(?:https?|wss?)://127\.0\.0\.1:[0-9]+[^\s]*`, "[LOCAL_ENDPOINT]"),
			mustPattern(`(?:/Users|/home)/[^\s]+`, "[LOCAL_PATH]"),
			mustPattern(`[A-Za-z]:\\Users\\[^\s]+`, "[LOCAL_PATH]"),
		},
	}
}

func mustPattern(pattern string, replacement string) redactionPattern {
	return redactionPattern{
		expr:        regexp.MustCompile(pattern),
		replacement: replacement,
	}
}

func (redactor Redactor) Redact(input string) string {
	output := input
	for _, pattern := range redactor.patterns {
		output = pattern.expr.ReplaceAllString(output, pattern.replacement)
	}
	return output
}

package jetstream

import (
	"testing"
	"time"
)

func TestConsumerRewindCursor(t *testing.T) {
	c := &Consumer{rewind: 5 * time.Second}

	cursor := int64(10_000_000)
	rewound := c.rewindCursor(&cursor)
	if rewound == nil || *rewound != 5_000_000 {
		t.Fatalf("rewound = %v, want 5000000", rewound)
	}

	cursor = 2_000_000
	rewound = c.rewindCursor(&cursor)
	if rewound == nil || *rewound != 0 {
		t.Fatalf("rewound = %v, want 0", rewound)
	}
}

package expire_test

import (
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/expire"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCollapse(t *testing.T) {
	// GT
	exp, err := expire.Collapse([]*expire.Expiry{
		{1 * time.Minute, expire.GT},
	})
	require.NoError(t, err)
	assert.EqualExportedValues(t, []*expire.Expiry{
		{1 * time.Minute, expire.GT},
	}, exp)

	// GT, NX
	exp, err = expire.Collapse([]*expire.Expiry{
		{1 * time.Minute, expire.GT},
		{2 * time.Minute, expire.NX},
	})
	require.NoError(t, err)
	assert.EqualExportedValues(t, []*expire.Expiry{
		{1 * time.Minute, expire.GT},
		{2 * time.Minute, expire.NX},
	}, exp)

	exp, err = expire.Collapse([]*expire.Expiry{
		{2 * time.Minute, expire.GT},
		{1 * time.Minute, expire.NX},
	})
	require.NoError(t, err)
	assert.EqualExportedValues(t, []*expire.Expiry{
		{2 * time.Minute, expire.GT},
		{1 * time.Minute, expire.NX},
	}, exp)

	// NX, GT
	exp, err = expire.Collapse([]*expire.Expiry{
		{1 * time.Minute, expire.NX},
		{2 * time.Minute, expire.GT},
	})
	require.NoError(t, err)
	assert.EqualExportedValues(t, []*expire.Expiry{
		{2 * time.Minute, expire.GT},
		{2 * time.Minute, expire.NX},
	}, exp)

	exp, err = expire.Collapse([]*expire.Expiry{
		{2 * time.Minute, expire.NX},
		{1 * time.Minute, expire.GT},
	})
	require.NoError(t, err)
	assert.EqualExportedValues(t, []*expire.Expiry{
		{1 * time.Minute, expire.GT},
		{2 * time.Minute, expire.NX},
	}, exp)

	// GT, NX, LT
	exp, err = expire.Collapse([]*expire.Expiry{
		{1 * time.Minute, expire.GT},
		{2 * time.Minute, expire.NX},
		{3 * time.Minute, expire.LT},
	})
	require.NoError(t, err)
	assert.EqualExportedValues(t, []*expire.Expiry{
		{1 * time.Minute, expire.GT},
		{2 * time.Minute, expire.NX},
		{3 * time.Minute, expire.LT},
	}, exp)

	exp, err = expire.Collapse([]*expire.Expiry{
		{1 * time.Minute, expire.GT},
		{3 * time.Minute, expire.NX},
		{2 * time.Minute, expire.LT},
	})
	require.NoError(t, err)
	assert.EqualExportedValues(t, []*expire.Expiry{
		{1 * time.Minute, expire.GT},
		{2 * time.Minute, expire.LT},
	}, exp)

	exp, err = expire.Collapse([]*expire.Expiry{
		{2 * time.Minute, expire.GT},
		{1 * time.Minute, expire.NX},
		{3 * time.Minute, expire.LT},
	})
	require.NoError(t, err)
	assert.EqualExportedValues(t, []*expire.Expiry{
		{2 * time.Minute, expire.GT},
		{1 * time.Minute, expire.NX},
		{3 * time.Minute, expire.LT},
	}, exp)

	exp, err = expire.Collapse([]*expire.Expiry{
		{2 * time.Minute, expire.GT},
		{3 * time.Minute, expire.NX},
		{1 * time.Minute, expire.LT},
	})
	require.NoError(t, err)
	assert.EqualExportedValues(t, []*expire.Expiry{
		{1 * time.Minute, expire.NONE},
	}, exp)

	exp, err = expire.Collapse([]*expire.Expiry{
		{3 * time.Minute, expire.GT},
		{1 * time.Minute, expire.NX},
		{2 * time.Minute, expire.LT},
	})
	require.NoError(t, err)
	assert.EqualExportedValues(t, []*expire.Expiry{
		{1 * time.Minute, expire.NX},
		{2 * time.Minute, expire.XX},
	}, exp)

	exp, err = expire.Collapse([]*expire.Expiry{
		{3 * time.Minute, expire.GT},
		{2 * time.Minute, expire.NX},
		{1 * time.Minute, expire.LT},
	})
	require.NoError(t, err)
	assert.EqualExportedValues(t, []*expire.Expiry{
		{1 * time.Minute, expire.NONE},
	}, exp)

	// GT, LT, NX
	exp, err = expire.Collapse([]*expire.Expiry{
		{1 * time.Minute, expire.GT},
		{2 * time.Minute, expire.LT},
		{3 * time.Minute, expire.NX},
	})
	require.NoError(t, err)
	assert.EqualExportedValues(t, []*expire.Expiry{
		{1 * time.Minute, expire.GT},
		{2 * time.Minute, expire.LT},
	}, exp)

	exp, err = expire.Collapse([]*expire.Expiry{
		{1 * time.Minute, expire.GT},
		{3 * time.Minute, expire.LT},
		{2 * time.Minute, expire.NX},
	})
	require.NoError(t, err)
	assert.EqualExportedValues(t, []*expire.Expiry{
		{1 * time.Minute, expire.GT},
		{3 * time.Minute, expire.LT},
	}, exp)

	exp, err = expire.Collapse([]*expire.Expiry{
		{2 * time.Minute, expire.GT},
		{1 * time.Minute, expire.LT},
		{3 * time.Minute, expire.NX},
	})
	require.NoError(t, err)
	assert.EqualExportedValues(t, []*expire.Expiry{
		{1 * time.Minute, expire.NONE},
	}, exp)

	exp, err = expire.Collapse([]*expire.Expiry{
		{2 * time.Minute, expire.GT},
		{3 * time.Minute, expire.LT},
		{1 * time.Minute, expire.NX},
	})
	require.NoError(t, err)
	assert.EqualExportedValues(t, []*expire.Expiry{
		{2 * time.Minute, expire.GT},
		{3 * time.Minute, expire.LT},
	}, exp)

	exp, err = expire.Collapse([]*expire.Expiry{
		{3 * time.Minute, expire.GT},
		{1 * time.Minute, expire.LT},
		{2 * time.Minute, expire.NX},
	})
	require.NoError(t, err)
	assert.EqualExportedValues(t, []*expire.Expiry{
		{1 * time.Minute, expire.NONE},
	}, exp)

	exp, err = expire.Collapse([]*expire.Expiry{
		{3 * time.Minute, expire.GT},
		{2 * time.Minute, expire.LT},
		{1 * time.Minute, expire.NX},
	})
	require.NoError(t, err)
	assert.EqualExportedValues(t, []*expire.Expiry{
		{2 * time.Minute, expire.NONE},
	}, exp)

	// NX, GT, LT
	exp, err = expire.Collapse([]*expire.Expiry{
		{1 * time.Minute, expire.NX},
		{2 * time.Minute, expire.GT},
		{3 * time.Minute, expire.LT},
	})
	require.NoError(t, err)
	assert.EqualExportedValues(t, []*expire.Expiry{
		{2 * time.Minute, expire.GT},
		{3 * time.Minute, expire.LT},
	}, exp)

	exp, err = expire.Collapse([]*expire.Expiry{
		{1 * time.Minute, expire.NX},
		{3 * time.Minute, expire.GT},
		{2 * time.Minute, expire.LT},
	})
	require.NoError(t, err)
	assert.EqualExportedValues(t, []*expire.Expiry{
		{2 * time.Minute, expire.NONE},
	}, exp)

	exp, err = expire.Collapse([]*expire.Expiry{
		{2 * time.Minute, expire.NX},
		{1 * time.Minute, expire.GT},
		{3 * time.Minute, expire.LT},
	})
	require.NoError(t, err)
	assert.EqualExportedValues(t, []*expire.Expiry{
		{1 * time.Minute, expire.GT},
		{2 * time.Minute, expire.NX},
		{3 * time.Minute, expire.LT},
	}, exp)

	exp, err = expire.Collapse([]*expire.Expiry{
		{2 * time.Minute, expire.NX},
		{3 * time.Minute, expire.GT},
		{1 * time.Minute, expire.LT},
	})
	require.NoError(t, err)
	assert.EqualExportedValues(t, []*expire.Expiry{
		{1 * time.Minute, expire.NONE},
	}, exp)

	exp, err = expire.Collapse([]*expire.Expiry{
		{3 * time.Minute, expire.NX},
		{1 * time.Minute, expire.GT},
		{2 * time.Minute, expire.LT},
	})
	require.NoError(t, err)
	assert.EqualExportedValues(t, []*expire.Expiry{
		{1 * time.Minute, expire.GT},
		{2 * time.Minute, expire.LT},
	}, exp)

	exp, err = expire.Collapse([]*expire.Expiry{
		{3 * time.Minute, expire.NX},
		{2 * time.Minute, expire.GT},
		{1 * time.Minute, expire.LT},
	})
	require.NoError(t, err)
	assert.EqualExportedValues(t, []*expire.Expiry{
		{1 * time.Minute, expire.NONE},
	}, exp)

	// NX, LT, GT
	exp, err = expire.Collapse([]*expire.Expiry{
		{1 * time.Minute, expire.NX},
		{2 * time.Minute, expire.LT},
		{3 * time.Minute, expire.GT},
	})
	require.NoError(t, err)
	assert.EqualExportedValues(t, []*expire.Expiry{
		{2 * time.Minute, expire.GT},
		{3 * time.Minute, expire.LT},
	}, exp)

	exp, err = expire.Collapse([]*expire.Expiry{
		{1 * time.Minute, expire.NX},
		{3 * time.Minute, expire.LT},
		{2 * time.Minute, expire.GT},
	})
	require.NoError(t, err)
	assert.EqualExportedValues(t, []*expire.Expiry{
		{2 * time.Minute, expire.NONE},
	}, exp)

	exp, err = expire.Collapse([]*expire.Expiry{
		{2 * time.Minute, expire.NX},
		{1 * time.Minute, expire.LT},
		{3 * time.Minute, expire.GT},
	})
	require.NoError(t, err)
	assert.EqualExportedValues(t, []*expire.Expiry{
		{1 * time.Minute, expire.GT},
		{2 * time.Minute, expire.NX},
		{3 * time.Minute, expire.LT},
	}, exp)

	exp, err = expire.Collapse([]*expire.Expiry{
		{2 * time.Minute, expire.NX},
		{3 * time.Minute, expire.LT},
		{1 * time.Minute, expire.GT},
	})
	require.NoError(t, err)
	assert.EqualExportedValues(t, []*expire.Expiry{
		{1 * time.Minute, expire.NONE},
	}, exp)

	exp, err = expire.Collapse([]*expire.Expiry{
		{3 * time.Minute, expire.NX},
		{1 * time.Minute, expire.LT},
		{2 * time.Minute, expire.GT},
	})
	require.NoError(t, err)
	assert.EqualExportedValues(t, []*expire.Expiry{
		{1 * time.Minute, expire.GT},
		{2 * time.Minute, expire.LT},
	}, exp)

	exp, err = expire.Collapse([]*expire.Expiry{
		{3 * time.Minute, expire.NX},
		{2 * time.Minute, expire.LT},
		{1 * time.Minute, expire.GT},
	})
	require.NoError(t, err)
	assert.EqualExportedValues(t, []*expire.Expiry{
		{1 * time.Minute, expire.NONE},
	}, exp)




	exp, err = expire.Collapse([]*expire.Expiry{
		{1 * time.Minute, expire.GT},
		{3 * time.Minute, expire.LT},
		{2 * time.Minute, expire.NX},
	})
	require.NoError(t, err)
	assert.EqualExportedValues(t, []*expire.Expiry{
		{1 * time.Minute, expire.GT},
		{3 * time.Minute, expire.LT},
	}, exp)

	exp, err = expire.Collapse([]*expire.Expiry{
		{1 * time.Minute, expire.GT},
		{3 * time.Minute, expire.LT},
		{2 * time.Minute, expire.NX},
	})
	require.NoError(t, err)
	assert.EqualExportedValues(t, []*expire.Expiry{
		{1 * time.Minute, expire.GT},
		{3 * time.Minute, expire.LT},
	}, exp)

	exp, err = expire.Collapse([]*expire.Expiry{
		{1 * time.Minute, expire.NX},
		{2 * time.Minute, expire.GT},
		{3 * time.Minute, expire.LT},
	})
	require.NoError(t, err)
	assert.EqualExportedValues(t, []*expire.Expiry{
		{2 * time.Minute, expire.GT},
		{2 * time.Minute, expire.NX},
		{3 * time.Minute, expire.LT},
	}, exp)

	exp, err = expire.Collapse([]*expire.Expiry{
		{1 * time.Minute, expire.XX},
		{2 * time.Minute, expire.NX},
	})
	require.NoError(t, err)
	assert.EqualExportedValues(t, []*expire.Expiry{
		{1 * time.Minute, expire.XX},
		{2 * time.Minute, expire.NX},
	}, exp)

	exp, err = expire.Collapse([]*expire.Expiry{
		{1 * time.Minute, expire.NX},
		{2 * time.Minute, expire.XX},
	})
	require.NoError(t, err)
	assert.EqualExportedValues(t, []*expire.Expiry{
		{2 * time.Minute, expire.XX},
	}, exp)

}

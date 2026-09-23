package storage

var (
	bucketMeta = []byte("meta")
	bucketLog  = []byte("log")

	keyTerm     = []byte("current_term")
	keyVotedFor = []byte("voted_for")
)

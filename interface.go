package srctype

// Connector
// Read() is iterator, return err: io.EOF at end
// ReadAll() return [][]string
type Connector interface {
	Read() ([]string, error) //iterator
	ColNames() ([]string, error)
	ReadAll() ([][]string, error)
	ReadStr() (string, error)
	ReadRowToChan(chan interface{})
	Close()
}

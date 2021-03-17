package daakiyaa

import (
	"fmt"

	"github.com/minhajuddinkhan/daakiya/storage"
)

func (d *daakiya) desynchronize(hash string, topic string) error {

	d.mutex.Lock()
	defer d.mutex.Unlock()
	k := d.getOffsetKey(hash, topic)
	d.synchronized[k] = false
	return nil
}

func (d *daakiya) synchronize(hash string, topic string) error {

	d.mutex.Lock()
	defer d.mutex.Unlock()
	k := d.getOffsetKey(hash, topic)

	if !d.synchronized[k] {
		o, err := d.store.GetLatestOffset(hash, topic)
		if err != nil {
			switch err.(type) {
			case *storage.OffsetNotFound:
				o = 0
			default:
				return err

			}
		}
		d.latestOffsets[k] = o + 1
		d.synchronized[k] = true
	}
	return nil
}

func (d *daakiya) own() {

	// d.store.Put()

}
func (d *daakiya) watchOwnershipChange() {}
func (d *daakiya) getOffsetKey(hash, topic string) string {
	return fmt.Sprintf("%s_%s", hash, topic)
}

package daakiyaa

import "fmt"

func (d *daakiya) synchronize(hash string, topic string) error {

	d.mutex.Lock()
	k := d.getOffsetKey(hash, topic)

	if !d.synchronized[k] {
		o, err := d.store.GetLatestOffset(hash, topic)
		if err != nil {
			d.mutex.Unlock()
			return err
		}
		d.latestOffsets[k] = o + 1
		d.synchronized[k] = true
	}
	d.mutex.Unlock()
	return nil

}

func (d *daakiya) getOffsetKey(hash, topic string) string {
	return fmt.Sprintf("%s_%s", hash, topic)
}

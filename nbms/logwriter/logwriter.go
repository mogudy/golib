package logwriter

import (
	"os"
	"sync"
	"time"
)

type RotateWriter struct {
	lock     sync.Mutex
	filename string
	fp       *os.File
}

func New(filename string) (*RotateWriter,error) {
	w := &RotateWriter{filename: filename}
	err := w.Rotate()
	return w,err
}
func (w *RotateWriter) Write(output []byte) (int, error) {
	w.lock.Lock()
	defer w.lock.Unlock()
	return w.fp.Write(output)
}
func (w *RotateWriter) Rotate() (err error) {
	w.lock.Lock()
	defer w.lock.Unlock()

	if w.fp != nil {
		err = w.fp.Close()
		if err != nil {
			return err
		}
		w.fp = nil
	}

	_, err = os.Stat(w.filename)
	if err == nil {
		err = os.Rename(w.filename, w.filename+"."+time.Now().Format("20060102T150405"))
		if err != nil {
			return err
		}
	}

	w.fp, err = os.Create(w.filename)
	return err
}

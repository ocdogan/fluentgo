package lib

import "time"

type FileInfo struct {
	name string
	date time.Time
	size uint64
}

func NewFileInfo(name string, date time.Time, size uint64) *FileInfo {
	return &FileInfo{
		name: name,
		date: date,
		size: size,
	}
}

func (f *FileInfo) Name() string {
	return f.name
}
func (f *FileInfo) Date() time.Time {
	return f.date
}

func (f *FileInfo) Size() uint64 {
	return f.size
}

type FileInfoList []*FileInfo

func (f FileInfoList) Len() int {
	return len(f)
}

func (f FileInfoList) Less(i, j int) bool {
	fi := f[i]
	fj := f[j]

	return fi.date.Sub(fj.date) < 0 ||
		fi.name < fj.name ||
		fi.size > fj.size
}

func (f FileInfoList) Swap(i, j int) {
	f[i], f[j] = f[j], f[i]
}

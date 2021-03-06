package main

import (
	"compress/bzip2"
	"database/sql"
	"encoding/xml"
	"errors"
	"flag"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"runtime"
	"runtime/pprof"
	"strings"
	"sync"
	"time"
)

type Location struct {
	Href string `xml:"href,attr"`
}

type ChecksumType struct {
	Value string `xml:",chardata"`
	Type  string `xml:"type,attr"`
}

type OpenChecksum struct {
	Value string `xml:",chardata"`
	Type  string `xml:"type,attr"`
}

type Data struct {
	Type         string       `xml:"type,attr"`
	Checksum     ChecksumType `xml:"checksum"`
	OpenChecksum OpenChecksum `xml:"open-checksum"`
	Location     Location     `xml:"location"`
	Timestamp    float64      `xml:"timestamp"`
	Size         uint32       `xml:"size"`
	OpenSize     uint32       `xml:"open-size"`
}

type RepomdXml struct {
	Repomd   string `xml:"repomd"`
	Revision int64  `xml:"revision"`
	Data     []Data `xml:"data"`
	name     string
	url      string
	dir      string
}

type BinPkg struct {
	bin string
	pkg string
}

type PkgKeyBins struct {
	pkgKey string
	bins   string
}

func getPkgKeyBins(file string) (pkb chan PkgKeyBins, err error) {
	pkb = make(chan PkgKeyBins)
	db, err := sql.Open("sqlite3", file)
	if err != nil {
		return
	}
	rows, err := db.Query("SELECT pkgKey,filenames FROM filelist WHERE dirname LIKE '/%bin' AND filetypes LIKE '%f';")
	if err != nil {
		return
	}
	go func() {
		defer os.Remove(file)
		defer db.Close()
		defer close(pkb)
		for rows.Next() {
			var p PkgKeyBins
			err = rows.Scan(&p.pkgKey, &p.bins)
			if err != nil {
				return
			}
			pkb <- p
		}
	}()
	return
}

func getBinPkg(file string, pkb chan PkgKeyBins) (bp chan BinPkg, err error) {
	bp = make(chan BinPkg)
	db, err := sql.Open("sqlite3", file)
	if err != nil {
		return
	}

	go func() {
		defer os.Remove(file)
		defer db.Close()
		defer close(bp)
		for p := range pkb {
			rows, err := db.Query("SELECT name FROM packages WHERE pkgKey=" + p.pkgKey + " AND arch !='i686';")
			if err != nil {
				return
			}
			for rows.Next() {
				var b BinPkg
				err = rows.Scan(&b.pkg)
				if err != nil {
					fmt.Println("row.Scan() failed", err)
					return
				}
				if b.pkg == "" {
					continue
				}
				bins := strings.Split(p.bins, "/")
				//fmt.Println("bins", bins)
				for _, bin := range bins {
					b.bin = bin
					bp <- b
				}
			}
		}
	}()
	return
}

func (r *RepomdXml) BinPkg() (bp chan BinPkg, err error) {
	var pkb chan PkgKeyBins
	pkbUpdate := make(chan bool)
	pbUpdate := make(chan bool)
	for _, d := range r.Data {
		switch d.Type {
		case "filelists_db":
			go func(d Data) {
				file, err := d.getDBFile(r.url)
				if err != nil {
					return
				}
				pkb, err = getPkgKeyBins(file)
				pkbUpdate <- true
			}(d)
		case "primary_db":
			go func(d Data) {
				file, err := d.getDBFile(r.url)
				if err != nil {
					return
				}
				<-pkbUpdate
				bp, err = getBinPkg(file, pkb)
				pbUpdate <- true
			}(d)
		}
	}
	<-pbUpdate
	return
}

func (r *RepomdXml) CreateCnfDB(bp <-chan BinPkg) (done chan bool, err error) {
	var wg sync.WaitGroup
	done = make(chan bool)
	tempsqlite := "/dev/shm/" + r.name + ".sqlite"
	db, err := sql.Open("sqlite3", tempsqlite)
	if err != nil {
		return
	}
	stmt, err := db.Prepare("CREATE TABLE cmdpkg(cmd TXT, pkg TXT, tips TXT);")
	if err == nil {
		_, e := stmt.Exec()
		if e != nil {
			fmt.Println("Exec()")
			return
		}
	}

	wg.Add(1)
	go func() {
		for b := range bp {
			st := "insert INTO cmdpkg(cmd,pkg) values('" + b.bin + "','" + b.pkg + "');"
			stmt, err := db.Prepare(st)
			if err != nil {
				panic("m")
			}
			_, err = stmt.Exec()
			if err != nil {
				panic("m")
			}
		}
		wg.Done()
	}()

	go func() {
		wg.Wait()
		db.Close()
		saved := r.dir + "/" + r.name + ".sqlite"
		sf, err := os.Open(tempsqlite)
		if err != nil {
			panic("os.Open() failed")
		}
		defer sf.Close()
		df, err := os.OpenFile(saved, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
		if err != nil {
			panic("os.OpenFile() failed")
		}
		if _, err = io.Copy(df, sf); err != nil {
			panic("io.Copy() failed")
		}
		defer df.Close()
		defer os.Remove(tempsqlite)
		done <- true
	}()
	return done, nil
}

// getDB() download and bunzip2
func (d *Data) getDBFile(url string) (file string, err error) {
	u := url + "/" + d.Location.Href
	resp, err := http.Get(u)
	if err != nil {
		return
	}
	reader := bzip2.NewReader(resp.Body)
	f, err := ioutil.TempFile("/dev/shm", d.Type+".")
	if err != nil {
		return
	}
	if _, err = io.Copy(f, reader); err != nil {
		return
	}
	defer f.Close()
	return f.Name(), nil
}

func NewRepomd(url string) (rx *RepomdXml, err error) {
	if url == "" {
		return nil, errors.New("url not specified")
	}
	fmt.Println("url", url)
	resp, err := http.Get(url + "/repodata/repomd.xml")
	if err != nil {
		return
	}
	rx = new(RepomdXml)
	err = xml.NewDecoder(resp.Body).Decode(rx)
	return
}

func main() {
	var url string
	mirror := flag.String("mirror", "http://mirrors.sohu.com", "mirror of package server")
	version := flag.String("version", "7", "version of distrobution")
	arch := flag.String("arch", "x86_64", "arch of distrobution, x86_64 or i386")
	dir := flag.String("dir", "database", "where DB saved")
	repo := flag.String("repo", "os", "repository, such as os, extras, centosplus...")
	repodata := flag.String("repodata", "", "repodata url")
	timeprofile := flag.Bool("time", false, "time profile")
	cpuprofile := flag.String("cpu", "", "cpu profile")

	nr := runtime.NumCPU()
	runtime.GOMAXPROCS(nr)
	flag.Parse()
	var t time.Time
	if *timeprofile {
		t = time.Now()
	}

	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(-4)
		}
		err = pprof.StartCPUProfile(f)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(-5)
		}
		defer pprof.StopCPUProfile()
	}
	if *repodata != "" {
		url = *repodata
	} else {
		url = *mirror + "/centos/" + *version + "/" + *repo + "/" + *arch
	}
	repomd, err := NewRepomd(url)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(-1)
	}
	repomd.dir = *dir
	repomd.url = url
	repomd.name = *repo
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(-1)
	}
	bp, err := repomd.BinPkg()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(-2)
	}
	done, err := repomd.CreateCnfDB(bp)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(-3)
	}

	<-done
	fmt.Println("OK, finished")
	if *timeprofile {
		fmt.Println("Total time: ", time.Since(t))
	}
	return
}

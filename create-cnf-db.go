package main

import (
	"compress/bzip2"
	"database/sql"
	"encoding/xml"
	"errors"
	"flag"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
	"sync"
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

var g_pkb = make(chan PkgKeyBins)
var g_bp = make(chan BinPkg)
var g_done = make(chan bool)

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
		defer close(pkb)
		for rows.Next() {
			var p PkgKeyBins
			err = rows.Scan(&p.pkgKey, &p.bins)
			if err != nil {
				return
			}
			fmt.Println("PkgKeyBins", p)
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
		defer close(bp)
		for p := range pkb {
			fmt.Println("p := <-pkb")
			rows, err := db.Query("SELECT name FROM packages WHERE pkgKey=" + p.pkgKey + " AND arch !='i686';")
			if err != nil {
				return
			}
			for rows.Next() {
				var b BinPkg
				err = rows.Scan(&b.pkg)
				if err != nil {
					return
				}
				//fmt.Println("BinPkg", b)
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
	for _, d := range r.Data {
		if d.Type == "filelists_db" {
			var file string
			file, err = d.getDBFile(r.url)
			if err != nil {
				return
			}
			pkb, err = getPkgKeyBins(file)
			if err != nil {
				return
			}
			fmt.Println("getPkgKeyBins return")
		}
	}
	for _, d := range r.Data {
		if d.Type == "primary_db" {
			fmt.Println("primary_db go func")
			var file string
			file, err = d.getDBFile(r.url)
			if err != nil {
				panic("d.getDBFile")
				return
			}
			bp, err = getBinPkg(file, pkb)
			if err != nil {
				panic("getBinPkg")
				return
			}
		}
	}
	return
}

func (r *RepomdXml) CreateCnfDB(bp <-chan BinPkg) (done chan bool, err error) {
	var wg sync.WaitGroup
	done = make(chan bool)
	db, err := sql.Open("sqlite3", "database/"+r.name+".sqlite")
	if err != nil {
		return
	}
	/*
		stmt, err := db.Prepare("CREATE TABLE cmdpkg(cmd TXT, pkg TXT, tips TXT);")
		if err != nil {
			fmt.Println("Prepare")
			return
		}
		res, err := stmt.Exec()
		if err != nil {
			fmt.Println("Exec()")
			return
		}
		_, err = res.RowsAffected()
		if err != nil {
			fmt.Println("RowsAffected()")
			return
		}
	*/
	go func() {
		wg.Add(1)
		for b := range bp {
			st := "insert INTO cmdpkg(cmd,pkg) values('" + b.bin + "','" + b.pkg + "');"
			stmt, err := db.Prepare(st)
			if err != nil {
				panic("m")
			}
			res, err := stmt.Exec()
			if err != nil {
				panic("m")
			}
			_, err = res.RowsAffected()
			if err != nil {
				panic("m")
			}
		}
		wg.Done()
	}()

	go func() {
		wg.Wait()
		done <- true
	}()
	return
}

// getDB() download and bunzip2
func (d *Data) getDBFile(url string) (file string, err error) {
	u := url + "/" + d.Location.Href
	fmt.Println("u", u, d)
	resp, err := http.Get(u)
	if err != nil {
		return
	}
	fmt.Println("bzip2", url)
	reader := bzip2.NewReader(resp.Body)
	buf, err := ioutil.ReadAll(reader)
	if err != nil {
		return
	}
	f, err := ioutil.TempFile("", d.Type+".")
	if err != nil {
		return
	}
	file = f.Name()
	err = ioutil.WriteFile(file, buf, 0644)
	if err != nil {
		return
	}
	fmt.Println("ioutil.WriteFile()", file)
	return
}

func NewRepomd(url string) (repo *RepomdXml, err error) {
	if url == "" {
		return nil, errors.New("url not specified")
	}
	fmt.Println("url", url)
	resp, err := http.Get(url + "/repodata/repomd.xml")
	if err != nil {
		return
	}
	repo = new(RepomdXml)
	err = xml.NewDecoder(resp.Body).Decode(repo)
	return
}

func main() {
	mirror := flag.String("mirror", "http://mirrors.sohu.com", "mirror of package server")
	version := flag.String("version", "7", "version of distrobution")
	arch := flag.String("arch", "x86_64", "arch of distrobution, x86_64 or i386")
	dir := flag.String("dir", "database", "where DB saved")

	flag.Parse()
	url := *mirror + "/centos/" + *version + "/os/" + *arch
	repo, err := NewRepomd(url)
	repo.dir = *dir
	repo.url = url
	repo.name = "os"
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(-1)
	}
	bp, err := repo.BinPkg()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(-2)
	}
	done, err := repo.CreateCnfDB(bp)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(-3)
	}

	for {
		select {
		case <-done:
			fmt.Println("OK, finished")
			os.Exit(0)
		}
	}
}
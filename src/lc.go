package main

import "os"
import "io"
import "fmt"
import "path"
import "bufio"
import "regexp"
import "io/ioutil"

func linecount(filename string, patterns []string) map[string]int {
	m := make(map[string]int)
	for _, pattern := range patterns {
		m[pattern] = 0
	}
	m["others"] = 0

	var recursive func(string)

	recursive = func (filename string) {
		fileinfo, err := os.Stat(filename)
		if err != nil {
			fmt.Println("cannot open file %s", filename)
			return
		}

		if fileinfo.IsDir() == false {
			cnt := 0
			file, err := os.Open(filename)
			if err != nil {
				return
			}
			fr := bufio.NewReader(file)
			for {
				_, _, err := fr.ReadLine()
				if err == io.EOF {
					break
				}
				cnt++
			}
			
			for _, pattern := range patterns {
				matched, _ := regexp.MatchString(pattern, filename)
				if matched == true {
					m[pattern] += cnt
					return
				}
			}
			m["others"] += cnt
		}

		files, _ := ioutil.ReadDir(filename)
		for _, file := range files {
			recursive(path.Join(filename, file.Name()))
		}
	}

	recursive(filename)

	return m
}

func main() {
	m := linecount(os.Args[1], os.Args[2:])
	for k, v := range m {
		fmt.Println(k, v)
	}
}

// Copyright (c) Dan Sheffner Digital Imaging Software Solutions, INC
//               Dan@Sheffner.com
// All rights reserved.
//
// Permission is hereby granted, free of charge, to any person obtaining a
// copy of this software and associated documentation files (the
// "Software"), to deal in the Software without restriction, including
// without limitation the rights to use, copy, modify, merge, publish, dis-
// tribute, sublicense, and/or sell copies of the Software, and to permit
// persons to whom the Software is furnished to do so, subject to the fol-
// lowing conditions:
//
// The above copyright notice and this permission notice shall be included
// in all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
// OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABIL-
// ITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT
// SHALL THE AUTHOR BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
// WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
// //IN THE SOFTWARE.

// This program will take a csv file and split it based on X number of lines
// This assumes you have a header row
// It will also compress them using gzip best compression.
// 2,000,000 million lines is approximately 250MB compressed

// Usage:
// go build main.go
// ./main file.csv prefix_

package main

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"
)

// Need this as a global since I'm going to add the header row to every new file
var toprow string

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func getFileName(e int) (filename string) {
	value := strconv.Itoa(e)
	length := len(value)

	if length == 1 {
		return "00000" + value
	} else if length == 2 {
		return "0000" + value
	} else if length == 3 {
		return "000" + value
	} else if length == 4 {
		return "00" + value
	} else if length == 5 {
		return "0" + value
	} else {
		return value
	}
}

func main() {
	start := time.Now()

	// what csv file do you want read
	file1, err := os.Open(os.Args[1])
	check(err)

	// prefix to use during file creation.
	// example if you do bla
	// you will get bla_0000001.csv.gz
	prefix := os.Args[2]

	defer file1.Close()

	scanner := bufio.NewScanner(file1)

	process_lines := 0
	line_num := 0
	file_num := 1
	threshold := 2000000

	// first file we are going to write to.
	filename := prefix + getFileName(file_num) + ".csv.gz"
	f, err := os.Create(filename)
	check(err)

	// create a new buffer.
	// Only do this once otherwise you will have problems
	w, e := gzip.NewWriterLevel(f, gzip.BestCompression)
	check(e)

	for scanner.Scan() {
		line := scanner.Text()

		if line_num == 0 {
			toprow = line
			//fmt.Println(toprow)
			_, err = w.Write([]byte(toprow + "\n"))
			check(err)
		}

		if line_num > threshold {
			line_num = 0
			file_num++

			// write to disk and close
			w.Flush()
			w.Close()

			// new file
			filename := prefix + getFileName(file_num) + ".csv.gz"
			f, err := os.Create(filename)
			check(err)

			// reset the buffer
			w.Reset(f)

			//fmt.Println(prefix + strconv.Itoa(file_num) + ".csv")
			_, err = w.Write([]byte(toprow + "\n"))
			check(err)
			_, err = w.Write([]byte(line + "\n"))
			check(err)

			//some output during the process
			fmt.Println("Processed: " + strconv.Itoa(process_lines - 1) + " lines with headers...")
		} else if line_num != 0 {
			_, err := w.Write([]byte(line + "\n"))
			check(err)
		}

		line_num++
		process_lines++
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
	// write to disk and close
	w.Flush()
	w.Close()

	elapsed := time.Since(start)
	fmt.Println("This script processed: " + strconv.Itoa(process_lines-1) + " lines")
	log.Printf("Took %s", elapsed)

}

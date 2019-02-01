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
	"io"
	"log"
	"os"
	"strconv"
	"time"
)

// Need this as a global since I'm going to add the header row to every new file
var toprow []byte

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func getFileName(e int) (filename string) {
	return fmt.Sprintf("%06d", e)
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

	// this breaks with too many lines
	//bufio.Scanner: token too long
	//scanner := bufio.NewScanner(file1)
	reader := bufio.NewReader(file1)

	processLines := 0
	lineNum := 0
	fileNum := 1
	threshold := 2000000

	endOfLine := "\n"
	var newline []byte
	newline2 := byte('\n')

	// first file we are going to write to.
	filename := prefix + getFileName(fileNum) + ".csv.gz"
	f, err := os.Create(filename)
	check(err)

	// create a new buffer.
	// Only do this once otherwise you will have problems
	w := gzip.NewWriter(f)

	for {
		line, err := reader.ReadSlice(newline2)

		//break out of loop at end of file
		if err == io.EOF {
			break
		}

		if lineNum == 0 {
			toprow = line
			//fmt.Println(toprow)
			newline = append(toprow, endOfLine...)
			_, err = w.Write(newline)
			check(err)
		}

		if lineNum > threshold {
			lineNum = 0
			fileNum++

			// write to disk and close
			w.Flush()
			w.Close()

			// new file
			filename := prefix + getFileName(fileNum) + ".csv.gz"
			f, err := os.Create(filename)
			check(err)

			// reset the buffer
			w.Reset(f)

			//fmt.Println(prefix + strconv.Itoa(fileNum) + ".csv")
			newline = append(toprow, endOfLine...)
			_, err = w.Write(newline)
			check(err)
			newline = append(line, endOfLine...)
			_, err = w.Write(newline)
			check(err)

			//some output during the process
			fmt.Println("Processed: " + strconv.Itoa(processLines-1) + " lines with headers...")
		} else if lineNum != 0 {
			newline = append(line, endOfLine...)
			_, err := w.Write([]byte(newline))
			check(err)
		}

		lineNum++
		processLines++
	}

	// write to disk and close
	w.Flush()
	w.Close()

	elapsed := time.Since(start)
	fmt.Println("This script processed: " + strconv.Itoa(processLines-1) + " lines")
	log.Printf("Took %s", elapsed)

}

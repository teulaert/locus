// Package synthpdf writes synthetic but well-formed PDF files for tests that
// need large documents: many pages of text, and optionally an embedded
// image per page so the file is large while carrying little text, like a
// scanned document. Output is streamed, so writing a file of tens of
// megabytes does not hold it in memory.
package synthpdf

import (
	"bufio"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"strings"
)

// Spec describes a synthetic PDF.
type Spec struct {
	// Pages is the number of pages (at least 1).
	Pages int
	// LinesPerPage lines of text are written on every page; 0 makes a
	// text-free (scanned-looking) document.
	LinesPerPage int
	// ImageBytes, when positive, embeds an image XObject of that many
	// bytes on every page (opaque DCT-labelled data the text extractor
	// never decodes).
	ImageBytes int
	// Seed varies the text and image bytes between files.
	Seed uint64
}

var words = strings.Fields(`pump station valve inspection quarterly report
maintenance schedule pressure reading flow meter river basin drainage
contract invoice delivery supplier warehouse calibration sensor network
north south east west culvert reservoir overflow spillway sediment sample`)

// Write streams the PDF described by s to w and returns its size.
func Write(w io.Writer, s Spec) (int64, error) {
	if s.Pages < 1 {
		s.Pages = 1
	}
	cw := &countWriter{w: bufio.NewWriterSize(w, 64<<10)}
	rng := rand.New(rand.NewPCG(s.Seed, s.Seed^0x9e3779b97f4a7c15))
	var offsets []int64 // offsets[i] is the offset of object i+1
	obj := func(body string) {
		offsets = append(offsets, cw.n)
		fmt.Fprintf(cw, "%d 0 obj\n%s\nendobj\n", len(offsets), body)
	}
	// Object numbers: 1 catalog, 2 pages, 3 font, then per page: page,
	// content and (with images) image.
	perPage := 2
	if s.ImageBytes > 0 {
		perPage = 3
	}
	pageObj := func(i int) int { return 4 + perPage*i }
	kids := make([]string, s.Pages)
	for i := range kids {
		kids[i] = fmt.Sprintf("%d 0 R", pageObj(i))
	}
	io.WriteString(cw, "%PDF-1.4\n")
	obj("<< /Type /Catalog /Pages 2 0 R >>")
	obj(fmt.Sprintf("<< /Type /Pages /Kids [%s] /Count %d >>", strings.Join(kids, " "), s.Pages))
	obj("<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>")
	img := make([]byte, s.ImageBytes)
	for i := 0; i < s.Pages; i++ {
		p := pageObj(i)
		res := "/Font << /F1 3 0 R >>"
		if s.ImageBytes > 0 {
			res += fmt.Sprintf(" /XObject << /Im1 %d 0 R >>", p+2)
		}
		obj(fmt.Sprintf("<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Resources << %s >> /Contents %d 0 R >>", res, p+1))
		var text strings.Builder
		if s.ImageBytes > 0 {
			text.WriteString("q 612 0 0 792 0 0 cm /Im1 Do Q\n")
		}
		if s.LinesPerPage > 0 {
			text.WriteString("BT\n/F1 10 Tf\n40 760 Td\n")
			for j := 0; j < s.LinesPerPage; j++ {
				if j > 0 {
					text.WriteString("0 -12 Td\n")
				}
				fmt.Fprintf(&text, "(Page %d line %d:", i+1, j+1)
				for k := 0; k < 12; k++ {
					text.WriteByte(' ')
					text.WriteString(words[rng.IntN(len(words))])
				}
				text.WriteString(") Tj\n")
			}
			text.WriteString("ET")
		}
		body := text.String()
		obj(fmt.Sprintf("<< /Length %d >>\nstream\n%s\nendstream", len(body), body))
		if s.ImageBytes > 0 {
			for k := range img {
				img[k] = byte(rng.Uint32())
			}
			offsets = append(offsets, cw.n)
			fmt.Fprintf(cw, "%d 0 obj\n<< /Type /XObject /Subtype /Image /Width 1000 /Height 1000 /ColorSpace /DeviceRGB /BitsPerComponent 8 /Filter /DCTDecode /Length %d >>\nstream\n", len(offsets), len(img))
			cw.Write(img)
			io.WriteString(cw, "\nendstream\nendobj\n")
		}
	}
	xref := cw.n
	fmt.Fprintf(cw, "xref\n0 %d\n0000000000 65535 f \n", len(offsets)+1)
	for _, off := range offsets {
		fmt.Fprintf(cw, "%010d 00000 n \n", off)
	}
	fmt.Fprintf(cw, "trailer\n<< /Size %d /Root 1 0 R >>\nstartxref\n%d\n%%%%EOF\n", len(offsets)+1, xref)
	if cw.err != nil {
		return cw.n, cw.err
	}
	return cw.n, cw.w.(*bufio.Writer).Flush()
}

// WriteFile writes the PDF described by s to path and returns its size.
func WriteFile(path string, s Spec) (int64, error) {
	f, err := os.Create(path)
	if err != nil {
		return 0, err
	}
	n, err := Write(f, s)
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	return n, err
}

type countWriter struct {
	w   io.Writer
	n   int64
	err error
}

func (c *countWriter) Write(p []byte) (int, error) {
	if c.err != nil {
		return 0, c.err
	}
	n, err := c.w.Write(p)
	c.n += int64(n)
	c.err = err
	return n, err
}

# DeX

## Caching

Handle initial retrieval and caching of the source CSV and EML docs that can be
processed by DeX.

Processing in DeX is always based on a single URL which references a CSV file in a PASTA
Data Package. When DeX receives a request to open a URL that it has not opened
previously, it requires the contents of the referenced CSV file along with the contents
of its associated EML file.

The CSV is available at the provided URL, and the EML is available at a predefined
location relative to the CSV, so both can be downloaded directly. However, the two files
may also be available in the local filesystem, and if so, should be used since it
improves performance.

During the initial request for a new CSV, DeX may need to refer to the CSV and EML
documents multiple times, so they should be kept cached locally for that time if they
had to be downloaded. On the other hand, if the files were already available in the
local filesystem, we want to use them from where they are, without copying them into a
local cache first.

During the initial request, the CSV and EML docs are processed into a number of other
objects which are cashed on disk. Only these derived objects are required for serving
later requests, so there is no longer a need for the original files.

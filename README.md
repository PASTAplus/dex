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


## Conda

### Managing the Conda environment in a production environment

Start and stop the dex service as root:

```shell
# systemctl start dex.service
# systemctl stop dex.service
```

Remove and rebuild the dex venv:

```shell
conda env remove --name dex
conda env create --file environment-min.yml
```

Update the dex venv in place:

```shell
conda env update --file environment-min.yml --prune
```

Activate and deactivate the dex venv:

```shell
conda activate dex
conda deactivate
```

### Managing the Conda environment in a development environment

Update the environment-min.yml:

```shell
conda env export --no-builds > environment-min.yml
```
Update Conda itself:

```shell
conda update --name base conda
```

Update all packages in environment:

```shell
conda update --all
```

Create or update the `requirements.txt` file (for use by GitHub Dependabot, and for pip based manual installs):

```shell
pip list --format freeze > requirements.txt
```

### Procedure for updating the Conda environment and all dependencies

```shell
conda activate base
conda env remove --name dex
conda update -n base -c conda-forge conda
conda env create --file environment-min.yml
conda activate dex
conda env export --no-builds > environment.yml
pip list --format freeze > requirements.txt
```
### If Conda base won't update to latest version, try:

```shell
conda update -n base -c defaults conda --repodata-fn=repodata.json
``` 

## API

### Flush cached objects for a given PackageID 

```shell
DELETE /<packageId>
```

#### Example:

Flush all cached objects for the package with the ID `https://pasta-d.lternet.edu/package/data/eml/edi/748/2`:

```shell
curl -X DELETE https://dex-d.edirepository.org/https%3A%2F%2Fpasta-d.lternet.edu%2Fpackage%2Fdata%2Feml%2Fedi%2F748%2F2
```

Note that the package ID is URL-encoded and that package scope, identifier and revision are all required, and separated by slashes.


### Open DeX with external data and metadata

DeX can be opened by providing links to a data table in CSV format, along with its associated metadata in EML format. DeX will download the data and metadata from the provided locations.

This API is used by posting a JSON document with the required information to `dex/api/preview`. DeX will return an identifier, which the browser can then use to form the complete URL to open.

Example JavaScript event handler for a button that opens DeX:

```javascript
window.onload = function () {
  document.getElementById('open-dex').addEventListener('click', function () {
    // Base URL for the DeX instance to use
    let dexBaseUrl = 'https://dex-d.edirepository.org';
    let data = {
      // Link to metadata document in EML format
      eml: 'https://pasta-s.lternet.edu/package/metadata/eml/edi/5/1',
      // Link to data table in CSV (or closely related) format
      csv: 'https://pasta-s.lternet.edu/package/data/eml/edi/5/1/88e508f7d25a90aa25b0159608187076',
      // As a single EML may contain metadata for multiple CSVs, this value is required and must
      // match the physical/distribution/online/url of the section in the EML which describes the
      // table.
      dist: 'https://pasta-s.lternet.edu/package/data/eml/edi/5/1/88e508f7d25a90aa25b0159608187076',
    };

    let options = {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json'
      },
      body: JSON.stringify(data),
    };

    fetch(`${dexBaseUrl}/dex/api/preview`, options)
        .then(response => {
          return response.text()
        })
        .then(body => {
          // Open DeX in new tab
          window.open(`${dexBaseUrl}/dex/profile/${body}`);
        })
        .catch(error => alert(error))
    ;
  });
};
```

# Hummus Datasets

This directory contains information about the publicly available `traceroute` datasets used by Hummus. 

## Sampled Ark and iPlane Datasets
Hummus uses sampled `Ark` (<http://www.caida.org/projects/ark/>) and `iPlane` (<http://iplane.cs.ucr.edu/data/data.html>) datasets from 2008 to 2013. 
In each month, we have collected one cycle's worth of data. The cycle length for `Ark` is 2-to-3 days, and it is one day for `iPlane`.

We have cleaned the data and stored them in compressed form in `Amazon S3` in the `arkiplane-2008-13-sampled.s3.amazonaws.com` bucket. You can get the list of files by issuing the following command:

	s3cmd ls -r s3://arkiplane-2008-13-sampled/

Of course, you'll have to install the fantastic `s3cmd` tool. You can download it from <http://s3tools.org/s3cmd>.

There are a total of 179 .gz files for `Ark` and 239 for `iPlane`.

## IP-to-AS and IP-to-GeoLocation Mapping Files
TBA.

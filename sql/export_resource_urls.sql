\copy (select metadata_blob, resource_blob from findopendata.socrata_resources) to 'socrata_resources.csv' with csv header;

\copy (select package_blob, resource_blob from findopendata.ckan_resources as a, findopendata.ckan_packages as b where a.package_key = b.key) to 'ckan_resources.csv' with csv header;

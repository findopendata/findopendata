import API_ENDPOINT from '../settings/API';
import handleErrors from './Util';


function FetchOriginalHosts(callBack) {
  fetch(`${API_ENDPOINT}/original-hosts`)
  .then(handleErrors)
  .then(res => res.json()).then((results) => {
    callBack(results);
  }).catch(err => {
      console.log(err);
  });
}

function FetchKeywordSearchTitleResults(query, selectedOriginalHosts, callBack) {
  var url_queries = [`query=${query}`,];
  if (selectedOriginalHosts.length > 0) {
    url_queries = url_queries.concat(selectedOriginalHosts.map(h => `original_host=${h}`));
  }
  fetch(`${API_ENDPOINT}/keyword-search-title?${url_queries.join('&')}`)
  .then(handleErrors)
  .then(res => res.json()).then((res) => {
    callBack(res);
  }).catch(err => {
    console.log(err);
  });
}

function FetchKeywordSearchResults(query, selectedOriginalHosts, callBack) {
  var url_queries = [`query=${query}`,];
  if (selectedOriginalHosts.length > 0) {
    url_queries = url_queries.concat(selectedOriginalHosts.map(h => `original_host=${h}`));
  }
  fetch(`${API_ENDPOINT}/keyword-search?${url_queries.join('&')}`)
  .then(handleErrors)
  .then(res => res.json()).then((res) => {
    callBack(res);
  }).catch(err => {
    console.log(err);
  });
}

function FetchSimilarPackageSearchResults(packageId, selectedOriginalHosts, callBack) {
  var url_queries = [`id=${packageId}`,];
  if (selectedOriginalHosts.length > 0) {
    url_queries = url_queries.concat(selectedOriginalHosts.map(h => `original_host=${h}`));
  }
  fetch(`${API_ENDPOINT}/similar-packages?${url_queries.join('&')}`)
  .then(handleErrors)
  .then(res => res.json()).then((res) => {
    callBack(res);
  }).catch(err => {
    console.log(err);
  });
}

function FetchPackage(packageId, callBackSuccess, callBackFail) {
  fetch(`${API_ENDPOINT}/package/${packageId}`)
  .then(handleErrors)
  .then(res => res.json())
  .then(res => {
    callBackSuccess(res);
  }).catch(err => {
    console.log(err);
    callBackFail(err);
  });
}

function FetchPackageFile(packageFileId, callBackSuccess, callBackFail) {
  fetch(`${API_ENDPOINT}/package-file/${packageFileId}`)
  .then(handleErrors)
  .then(res => res.json())
  .then(res => {
    callBackSuccess(res);
  }).catch(err => {
    console.log(err);
    callBackFail(err);
  });
}

function FetchPackageFileData(packageFileId, callBackSuccess, callBackFail) {
  fetch(`${API_ENDPOINT}/package-file-data/${packageFileId}`)
  .then(handleErrors)
  .then(res => res.json())
  .then(res => {
    callBackSuccess(res);
  }).catch(err => {
    console.log(err);
    callBackFail(err);
  });
}

function FetchJoinableColumns(columnId, selectedOriginalHosts, callBackSuccess, callBackFail) {
  var url_queries = [`id=${columnId}`,];
  if (selectedOriginalHosts.length > 0) {
    url_queries = url_queries.concat(selectedOriginalHosts.map(h => `original_host=${h}`));
  }
  fetch(`${API_ENDPOINT}/joinable-column-search?${url_queries.join('&')}`)
  .then(handleErrors)
  .then(res => res.json()).then((res) => {
    callBackSuccess(res);
  }).catch(err => {
    console.log(err);
    callBackFail(err);
  });
}

export {
  FetchOriginalHosts,
  FetchKeywordSearchResults, 
  FetchKeywordSearchTitleResults, 
  FetchSimilarPackageSearchResults,
  FetchPackage,
  FetchPackageFile,
  FetchPackageFileData,
  FetchJoinableColumns,
};
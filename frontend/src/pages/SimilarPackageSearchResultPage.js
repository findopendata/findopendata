import React from 'react';
import PackageSearchResults from '../components/PackageSearchResults';
import { FetchSimilarPackageSearchResults } from '../tools/RemoteData';


class SimilarPackageSearchResultPage extends React.Component {
  constructor(props) {
    super(props);
    this.state = {
      processing : false,
      searchResults : [],
      query_id: null,
    };
  }
  fetchSearchResults() {
    this.setState({ processing: true });
    let params = new URLSearchParams(this.props.location.search);
    const packageId = params.get("id");
    const hosts = this.props.selectedOriginalHosts.map(h => h.original_host);
    FetchSimilarPackageSearchResults(packageId, hosts, (res) => {
      this.setState({
        processing: false,
        searchResults: res,
        query_id: packageId,
      });
    });
  }
  componentDidMount() {
    this.fetchSearchResults();
  }
  componentDidUpdate(prevProps) {
    if (this.props.location !== prevProps.location || 
      this.props.selectedOriginalHosts !== prevProps.selectedOriginalHosts) {
      this.fetchSearchResults();
    }
  }
  isHighlighted(pac) {
    return this.state.query_id === pac.id;
  }
  render() {
    return (
      <PackageSearchResults 
        processing={this.state.processing}
        searchResults={this.state.searchResults}
        originalHosts={this.props.originalHosts}
        selectedOriginalHosts={this.props.selectedOriginalHosts}
        onClickSelectOriginalHost={this.props.onClickSelectOriginalHost}
        onClickUnselectOriginalHost={this.props.onClickUnselectOriginalHost}
        onClickPin={this.props.onClickPin}
        isPinned={this.props.isPinned}
        isHighlighted={this.isHighlighted.bind(this)}
        onPackageLoad={this.props.onPackageLoad}
      />
    );
  }
}

export default SimilarPackageSearchResultPage;
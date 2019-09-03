import React from 'react';
import {
  Container, Row,
} from 'react-bootstrap';
import { BrowserRouter as Router, Route } from "react-router-dom";
import './App.css';
import Topbar from './components/Topbar';
import Sidebar from './components/Sidebar';
import PackageDetail from './components/PackageDetail';
import KeywordSearchResultPage from './pages/KeywordSearchResultPage';
import PackageFile from './components/PackageFile';
import HomePage from './pages/HomePage';
import { FetchOriginalHosts } from './tools/RemoteData';
import SimilarPackageSearchResultPage from './pages/SimilarPackageSearchResultPage';

class App extends React.Component {
  constructor(props) {
    super(props);
    this.state = {
      searchHistory: [],
      pinnedPackages: [],
      recentPackages: [],
      originalHosts: [],
      selectedOriginalHosts: [],
    };
  }
  fetchOriginalHosts() {
    FetchOriginalHosts((res) => {
      this.setState({
        originalHosts: res,
      });
    })
  }
  componentDidMount() {
    this.fetchOriginalHosts();
  }
  handleAddOriginalHost(originalHost) {
    const h = this.state.originalHosts.find(host => host.original_host === originalHost);
    if (h === undefined) {
      return;
    }
    if (!this.state.selectedOriginalHosts.map(host => host.original_host).includes(originalHost)) {
      this.setState({
        selectedOriginalHosts: this.state.selectedOriginalHosts.concat([h]),
      });
    }
  }
  handleRemoveOriginalHost(originalHost) {
    const index = this.state.selectedOriginalHosts.map(host => host.original_host).indexOf(originalHost);
    if (index > -1) {
      this.setState({
        selectedOriginalHosts: this.state.selectedOriginalHosts.filter(host => host.original_host !== originalHost),
      });
    }
  }
  handleKeywordSearch(query) {
    const history = this.state.searchHistory;
    this.setState({ searchHistory: [query,].concat(history) });
  }
  isPinned(p) {
    return this.state.pinnedPackages.filter(pac => pac.id === p.id).length;
  }
  handlePinningPackage(pac) {
    const index = this.state.pinnedPackages.findIndex(p => p.id === pac.id);
    if (index > -1) {
      // Unpin
      this.setState({
        pinnedPackages : this.state.pinnedPackages.filter(p => p.id !== pac.id),
      });
    } else {
      // Pin
      this.setState({
        pinnedPackages: this.state.pinnedPackages.concat([pac]),
      });
    }
  }
  handleAddRecentPackage(pac) {
    const index = this.state.recentPackages.findIndex(p => p.id === pac.id);
    if (index > -1) {
      // Remove
      this.setState({
        recentPackages: this.state.recentPackages.filter(p => p.id !== pac.id),
      })
    }
    // Prepend
    this.setState({
      recentPackages: [pac,].concat(this.state.recentPackages),
    });
  }
  render() {
    return (
      <Router>
        <div>
          <Topbar
            handleKeywordSearch={this.handleKeywordSearch.bind(this)}
          />
          <Container fluid>
            <Row>
              <Sidebar 
                className="col-md-2 d-none d-md-block sidebar"
                pinnedPackages={this.state.pinnedPackages} 
                recentPackages={this.state.recentPackages}
              />
              <main className="col-md-9 ml-sm-auto col-lg-10 px-4" role="main">
                <Route exact path="/" component={HomePage} />
                <Route path="/keyword-search"
                  render={(routeProps) =>
                    <KeywordSearchResultPage
                      {...routeProps}
                      originalHosts={this.state.originalHosts}
                      selectedOriginalHosts={this.state.selectedOriginalHosts}
                      onClickSelectOriginalHost={this.handleAddOriginalHost.bind(this)}
                      onClickUnselectOriginalHost={this.handleRemoveOriginalHost.bind(this)}
                      isPinned={this.isPinned.bind(this)}
                      onClickPin={this.handlePinningPackage.bind(this)}
                      onPackageLoad={this.handleAddRecentPackage.bind(this)}
                    />
                  }
                />
                <Route path="/similar-packages"
                  render={(routeProps) =>
                    <SimilarPackageSearchResultPage
                      {...routeProps}
                      originalHosts={this.state.originalHosts}
                      selectedOriginalHosts={this.state.selectedOriginalHosts}
                      onClickSelectOriginalHost={this.handleAddOriginalHost.bind(this)}
                      onClickUnselectOriginalHost={this.handleRemoveOriginalHost.bind(this)}
                      isPinned={this.isPinned.bind(this)}
                      onClickPin={this.handlePinningPackage.bind(this)}
                      onPackageLoad={this.handleAddRecentPackage.bind(this)}
                    />
                  }
                />
                <Route path="/package/:id"
                  render={(routeProps) =>
                    <PackageDetail
                      {...routeProps}
                      pacId={routeProps.match.params.id}
                      onLoad={this.handleAddRecentPackage.bind(this)}
                      onClickPin={this.handlePinningPackage.bind(this)}
                      isPinned={this.isPinned.bind(this)}
                    />
                  }
                />
                <Route path="/package-file/:id"
                  render={(routeProps) =>
                    <PackageFile
                      {...routeProps}
                      pacFileId={routeProps.match.params.id}
                      onLoad={this.handleAddRecentPackage.bind(this)}
                      onClickPin={this.handlePinningPackage.bind(this)}
                      isPinned={this.isPinned.bind(this)}
                    />
                  }
                />
              </main>
            </Row>
          </Container>
        </div>
      </Router>
    );
  }
}

export default App;

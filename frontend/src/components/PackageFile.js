import React from 'react';
import {
  Table, ButtonToolbar, ButtonGroup, Button, Spinner,
} from 'react-bootstrap';
import { Link } from "react-router-dom";
import { FetchPackageFileData, FetchPackageFile } from '../tools/RemoteData';


class PackageFile extends React.Component {
  constructor(props) {
    super(props);
    this.state = {
      pac: {},
      file: {},
      headers: [],
      records: [],
      loadingRecords: false,
    };
  }
  fetchPackageFile(id) {
    FetchPackageFile(id, (res) => {
      this.setState({
        pac: res['package'],
        file: res['package_file'],
      });
      this.props.onLoad(this.state.pac);
    });
  }
  fetchPackageFileData(id) {
    this.setState({ loadingRecords: true });
    FetchPackageFileData(id, (res) => {
      this.setState({
        records: res['records'],
        headers: res['headers'],
        loadingRecords: false,
      });
    }, (err) => {
      this.setState({
        loadingRecords: false,
      })
    });
  }
  componentDidMount() {
    this.fetchPackageFile(this.props.pacFileId);
    this.fetchPackageFileData(this.props.pacFileId);
  }
  componentDidUpdate(prevProps) {
    if (this.props.pacFileId !== prevProps.pacFileId) {
      this.fetchPackageFile(this.props.pacFileId);
      this.fetchPackageFileData(this.props.pacFileId);
    }
  }
  render() {
    const file = this.state.file;
    const pac = this.state.pac;
    const headers = this.state.headers;
    const records = this.state.records;
    return (
      <div>
        <div className="d-flex justify-content-between flex-wrap flex-md-nowrap align-items-center pt-3 pb-2 mb-3 border-bottom">
          <h2>{file.filename}</h2>
          <ButtonToolbar className="mb-2 mb-md-0">
            <ButtonGroup className="mr-2">
              <Link className="btn btn-outline-secondary btn-sm"
                to={`/similar-packages?id=${pac.id}`}
              >Find Similar Packages</Link>
              <Button variant="outline-secondary" size="sm"
                onClick={() => this.props.onClickPin(pac)}
                active={this.props.isPinned(pac)}
                >Pin Package</Button>
            </ButtonGroup>
          </ButtonToolbar>
        </div>
        <p className="text-muted">
          {pac.original_host_display_name} - {
            file.modified ? (<span>Updated: {file.modified}</span>) : 
              (<span>Created: {file.created}</span>) 
          }
        </p>
        <p>{file.description}</p>
        <p>Go to package: <Link to={`/package/${pac.id}`}>{pac.title}</Link>.</p>
        <p>Showing only the first {records.length} rows.</p>
        <div className={this.state.loadingRecords ? 'processing-loading' : 'processing-done'}>
          <div className="d-flex justify-content-center my-10 py-10">
            <Spinner animation="border" role="status">
              <span className="sr-only">Loading...</span>
            </Spinner>
          </div>
        </div>
        <Table responsive striped bordered hover>
          <thead>
            <tr>
              { headers.map(h => <th key={`header:${h}`}>{h}</th>) }
            </tr>
          </thead>
          <tbody>
            {
              records.map((record, i) => (
                <tr key={`row:${i}`}>
                  {
                    headers.map((key, j) => (
                      <td key={`cell:${i}:${j}`}>
                        { 
                          record[key] ? (
                            typeof(record[key]) === "object" ? 
                              JSON.stringify(record[key]) : record[key]
                          ) : ''
                        }
                      </td>
                    ))
                  }
                </tr>
              ))
            }
          </tbody>
        </Table>
      </div>
    );
  }
}

export default PackageFile;
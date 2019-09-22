import React from 'react';
import {
  Table, ButtonToolbar, ButtonGroup, Button, Spinner, Modal, OverlayTrigger, Tooltip,
} from 'react-bootstrap';
import { Link } from "react-router-dom";
import { FetchPackageFileData, FetchPackageFile } from '../tools/RemoteData';
import JoinableColumnSearchResult from './JoinableColumnSearchResult';


class PackageFile extends React.Component {
  constructor(props) {
    super(props);
    this.state = {
      pac: {},
      file: {},
      columns: [],
      records: [],
      loadingRecords: false,
      showingColumnSearchResult: false,
      queryColumn: {},
    };
  }
  handleCloseColumnSearchResult() {
    this.setState({queryColumn: {}, showingColumnSearchResult: false});
  }
  handleOpenColumnSearchResult(queryColumn) {
    this.setState({queryColumn: queryColumn, showingColumnSearchResult: true});
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
        columns: res['columns'],
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
    const columns = this.state.columns;
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
              { 
                columns.map(h => 
                  <OverlayTrigger
                    placement="bottom"
                    overlay={
                      <Tooltip>
                        Click me to find joinable tables on this column.
                      </Tooltip>
                    }
                  >
                    <th key={`column:${h.column_name}`} 
                      onClick={() => this.handleOpenColumnSearchResult(h)}
                      className="column"
                    >
                      {h.column_name}
                    </th>
                  </OverlayTrigger>
                ) 
              }
            </tr>
          </thead>
          <tbody>
            {
              records.map((record, i) => (
                <tr key={`row:${i}`}>
                  {
                    columns.map(c => c.column_name).map((key, j) => (
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
        <Modal size="lg" aria-labelledby="contained-modal-title-vcenter"
          centered 
          show={this.state.showingColumnSearchResult}
          onHide={() => this.handleCloseColumnSearchResult()}
          >
          <Modal.Body>
            <JoinableColumnSearchResult 
              columnId={this.state.queryColumn.id}
              columnName={this.state.queryColumn.column_name}
              onClickResult={this.handleCloseColumnSearchResult.bind(this)}
            />
          </Modal.Body>
        </Modal>
      </div>
    );
  }
}

export default PackageFile;
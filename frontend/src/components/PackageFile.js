import React from 'react';
import {
  ButtonToolbar, ButtonGroup, Button
} from 'react-bootstrap';
import { Link } from "react-router-dom";
import { FetchPackageFile } from '../tools/RemoteData';
import { LoadingSpinner } from './LoadingSpinner';
import DataTable from './DataTable';


class PackageFile extends React.Component {
  constructor(props) {
    super(props);
    this.state = {
      pac: {},
      file: {},
      loading: false,
    };
  }
  fetchPackageFile(id) {
    this.setState({loading: true});
    FetchPackageFile(id, (res) => {
      this.setState({
        pac: res['package'],
        file: res['package_file'],
        loading: false,
      });
      this.props.onLoad(this.state.pac);
    }, (err) => {
      this.setState({
        loading: false,
      })
    });
  }
  componentDidMount() {
    this.fetchPackageFile(this.props.pacFileId);
  }
  componentDidUpdate(prevProps) {
    if (this.props.pacFileId !== prevProps.pacFileId) {
      this.fetchPackageFile(this.props.pacFileId);
    }
  }
  render() {
    const file = this.state.file;
    const pac = this.state.pac;
    const sampleLength = file.sample ? file.sample.length : 0;
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
        <LoadingSpinner loading={this.state.loading} />
        <p>{file.description}</p>
        <p>Go to package: <Link to={`/package/${pac.id}`}>{pac.title}</Link>.</p>
        <p>Showing only the first {sampleLength} rows, <a href={file.original_url}>download the file</a>.</p>
        <DataTable columns={file.columns} records={file.sample} />
      </div>
    );
  }
}

export default PackageFile;
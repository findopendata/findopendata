import React from 'react';
import {
  ButtonToolbar, ButtonGroup, Button, Badge, CardColumns, Card,
} from 'react-bootstrap';
import { Link } from "react-router-dom";
import { FetchPackage } from '../tools/RemoteData';
import { LoadingSpinner } from './LoadingSpinner';

class PackageDetail extends React.Component {
  constructor(props) {
    super(props);
    this.state = {
      pac: { tags: [] },
      files: [],
      loading: false,
    };
  }
  fetchPackageDetail(id) {
    if (!id) {
      return;
    }
    this.setState({loading: true});
    FetchPackage(id, (res) => {
      this.setState({
        pac: res['package'],
        files: res['package_files'],
        loading: false,
      });
      this.props.onLoad(this.state.pac);
    }, (err) => {
      this.setState({loading: false});
    });
  }
  componentDidMount() {
    this.fetchPackageDetail(this.props.pacId);
  }
  componentDidUpdate(prevProps) {
    if (this.props.pacId !== prevProps.pacId) {
      this.fetchPackageDetail(this.props.pacId);
    }
  }
  render() {
    const pac = this.state.pac;
    const files = this.state.files.sort((f1, f2) => {
      if (f1.filename < f2.filename) {
        return -1;
      }
      if (f1.filename > f2.filename) {
        return 1;
      }
      return 0;
    });
    return (
      <div>
        <div className="d-flex justify-content-between flex-wrap flex-md-nowrap align-items-center pt-4 pb-2 mb-3 border-bottom">
          <h2>{pac.title}</h2>
          <ButtonToolbar className="mb-2 mb-md-0">
            <ButtonGroup className="mr-2">
              <Link className="btn btn-outline-secondary btn-sm"
                to={`/similar-packages?id=${pac.id}`}
              >Find Similar</Link>
              <Button variant="outline-secondary" size="sm"
                onClick={() => this.props.onClickPin(pac)}
                active={this.props.isPinned(pac)}
                >Pin</Button>
            </ButtonGroup>
          </ButtonToolbar>
        </div>
        <p className="text-muted">
          {pac.original_host_display_name} - {
            pac.modified ? (<span>Updated: {pac.modified}</span>) : 
              (<span>Created: {pac.created}</span>) 
          }
        </p>
        <LoadingSpinner loading={this.state.loading} />
        <h3>Publisher</h3>
        <p>{pac.organization_display_name}</p>
        <h3>Description</h3>
        <p>{pac.description}</p>
        <h3>Tags</h3>
        <div className="d-flex flex-wrap py-2">
          {
            pac.tags.map(t => <Badge key={t} className="d-inline-flex m-1 p-2" variant="secondary">{t}</Badge>)
          }
        </div>
        <h3>License</h3>
        <p><a href={pac.license_url}>{pac.license_title}</a></p>
        <h3>Files</h3>
        <CardColumns>
          {
            files.map((f) => 
              <Card key={f.id}>
                <Card.Header>{f.format}</Card.Header>
                <Card.Body>
                  <Card.Title>{f.name}</Card.Title>
                  <Card.Subtitle className="mb-2 text-muted">
                    Updated: {f.modified ? f.modified : f.created}
                  </Card.Subtitle>
                  <Card.Text>
                  {f.filename}
                  </Card.Text>
                  {
                   f.available ? <Link className="btn btn-primary btn-sm m-2" to={`/package-file/${f.id}`}>Preview</Link> : ''
                  }
                  <a className="btn btn-info btn-sm m-2" href={f.original_url}>Download</a>
                </Card.Body>
              </Card>
            )
          }
        </CardColumns>
      </div>
    );
  }
}

export default PackageDetail;
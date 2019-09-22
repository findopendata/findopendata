import React from 'react';
import { FetchJoinableColumns } from '../tools/RemoteData';
import { Card, Spinner } from 'react-bootstrap';
import { Link } from "react-router-dom";
import TextTruncate from 'react-text-truncate';

class JoinableColumnSearchResult extends React.Component {
  constructor(props) {
    super(props);
    this.state = {
      columns: [],
      loading: false,
    };
  }
  fetchJoinableColumns(columnId) {
    if (!columnId) {
      return;
    }
    this.setState({ loading: true });
    FetchJoinableColumns(columnId, [], (res) => {
      this.setState({
        columns: res,
        loading: false,
      });
    }, (err) => {
      this.setState({
        loading: false,
      });
    });
  }
  componentDidMount() {
    this.fetchJoinableColumns(this.props.columnId)
  }
  componentDidUpdate(prevProps) {
    if (this.props.columnId !== prevProps.columnId) {
      this.fetchJoinableColumns(this.props.columnId);
    }
  }
  render() {
    const columns = this.state.columns;
    return (
      <div>
        <div className="d-flex justify-content-between flex-wrap flex-md-nowrap align-items-center pt-3 pb-2 mb-3 border-bottom">
          <h3>Joinable Tables on <em>{this.props.columnName}</em></h3>
        </div>
        <p>Found {columns.length} joinable tables.</p>
        <div className={this.state.loading ? 'processing-loading' : 'processing-done'}>
          <div className="d-flex justify-content-center my-10 py-10">
            <Spinner animation="border" role="status">
              <span className="sr-only">Loading...</span>
            </Spinner>
          </div>
        </div>
        <div>
          {
            columns.map((column) => 
              <Card key={column.id}
                className="column-card mb-3">
                <Card.Header>
                  {column.organization_display_name} - {column.original_host_display_name}
                </Card.Header>
                <Card.Body>
                  <Card.Title>
                    <Link 
                      to={`/package-file/${column.package_file_id}`}
                      onClick={() => this.props.onClickResult()}
                    >
                      {column.filename}
                    </Link> &rarr; {column.column_name}
                  </Card.Title>
                  <Card.Subtitle className="mb-2 text-muted">
                    Package: {column.package_title}
                  </Card.Subtitle>
                  <TextTruncate
                    line={6}
                    truncateText="..."
                    text={column.package_description}
                    containerClassName="card-text"
                  />
                </Card.Body>
                <Card.Footer>
                  Similarity: {column.jaccard}
                </Card.Footer>
              </Card>
            )
          }
        </div>
      </div>
    );
  }
}

export default JoinableColumnSearchResult;
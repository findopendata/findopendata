import React from 'react';
import {
  Table, Modal, OverlayTrigger, Tooltip,
} from 'react-bootstrap';
import JoinableColumnSearchResult from './JoinableColumnSearchResult';


class DataTable extends React.Component {
  constructor(props) {
    super(props);
    this.state = {
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
  emptyArrayIfNull(a) {
    return a ? a : [];
  }
  getDefault(a, d) {
    return a ? a : d;
  }
  render() {
    var maxColumns = this.getDefault(this.props.maxColumns, -1);
    var maxRecords = this.getDefault(this.props.maxRecords, -1);
    const fullColumns = this.emptyArrayIfNull(this.props.columns);
    const fullRecords = this.emptyArrayIfNull(this.props.records); 
    const columns = fullColumns.slice(0, maxColumns);
    const records = fullRecords.slice(0, maxRecords);
    return (
      <div>
        <Table responsive striped bordered hover>
          <thead>
            <tr>
              { 
                columns.map(h => 
                  h.id ? (
                    <OverlayTrigger
                      key={`column-overlay:${h.column_name}`}
                      placement="bottom"
                      overlay={
                        <Tooltip>
                          Click me to find joinable tables on this column.
                        </Tooltip>
                      }
                    >
                      <th key={`column:${h.column_name}`} 
                        onClick={() => this.handleOpenColumnSearchResult(h)}
                        className="column-search"
                      >
                        {h.column_name}
                      </th>
                    </OverlayTrigger>
                  ) : (
                      <th key={`column:${h.column_name}`} 
                        className="column-no-search"
                      >
                        {h.column_name}
                      </th>
                  )
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

export default DataTable;
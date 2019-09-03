import React from 'react';
import { Navbar, Badge, Dropdown, FormControl } from 'react-bootstrap';


class FilterBar extends React.Component {
  constructor(props) {
    super(props);
    this.state = {value: ''};
  }
  handleChange(e) {
    this.setState({ value: e.target.value.toLowerCase().trim() });
  }
  render() {
    const filters = this.props.filters;
    return (
      <Navbar className={this.props.className}>
        <Dropdown>
          <Dropdown.Toggle variant="secondary" id="dropdown-custom-components">
            {this.props.title}
          </Dropdown.Toggle>
          <Dropdown.Menu className="border shadow-sm">
            <div className="sticky-top filter-form">
              <FormControl
                autoFocus
                className="mx-3 my-2 w-auto border"
                placeholder="Type to filter..."
                onChange={this.handleChange.bind(this)}
                value={this.state.value}
              />
            </div>
            <div className="filter-items">
              {
                filters.filter((f => !this.state.value || 
                    f.name.toLowerCase().includes(this.state.value))).map((f) =>
                  <Dropdown.Item 
                    key={f.key} 
                    active={f.selected}
                    onClick={(event => {
                      event.preventDefault();
                      if (f.selected) {
                        this.props.onClickUnselectFilter(f.key);
                      } else {
                        this.props.onClickSelectFilter(f.key);
                      }
                    })}
                    >
                    {f.name} 
                  </Dropdown.Item>
                )
              }
            </div>
          </Dropdown.Menu>
        </Dropdown>
        <div className="px-1 d-flex flex-wrap">
          {
            filters.filter(f => f.selected).map(f => 
              <Badge pill variant="info" className="d-inline-flex m-1 p-2 tag" key={f.key}
                onClick={(event) => {
                  event.preventDefault();
                  this.props.onClickUnselectFilter(f.key);
                }}
                >
                { f.name } &times;
              </Badge>
            )
          }
        </div>
      </Navbar>
    );
  }
}

export default FilterBar;
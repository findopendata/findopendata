import React from 'react';
import { FormControl } from 'react-bootstrap';
import { withRouter } from "react-router-dom";


class KeywordSearchForm extends React.Component {
  constructor(props) {
    super(props);
    let params = new URLSearchParams(this.props.location.search);
    const query = params.get("query");
    this.state = { value: query === null ? '' : query };
    this.handleChange = this.handleChange.bind(this);
    this.handleKeyPress = this.handleKeyPress.bind(this);
  }

  handleChange(event) {
    this.setState({value: event.target.value});
  }

  handleKeyPress(event) {
    if (event.key === 'Enter') {
      // Fetch search results.
      this.props.handleKeywordSearch(this.state.value);
      // Route to result page.
      this.props.history.push('/keyword-search?query='+this.state.value);
    }
  }

  render() {
    return (
        <FormControl
          variant="dark"
          className="w-100"
          type="text"
          placeholder="Search"
          aria-label="Search"
          value={this.state.value}
          onChange={this.handleChange}
          onKeyPress={this.handleKeyPress}
        />
    );
  }
}

export default withRouter(KeywordSearchForm);


import React from 'react';
import { Navbar } from 'react-bootstrap';
import { Link } from 'react-router-dom';
import KeywordSearchForm from './KeywordSearchForm.js'


class Topbar extends React.Component {
  render() {
    return (
      <Navbar fixed="top" bg="dark" variant="dark" className="flex-md-nowrap p-0 shadow">
        <Link to="/" className="navbar-brand col-sm-3 col-md-2 mr-0">
          Find Open Data
        </Link>
        <KeywordSearchForm
          handleKeywordSearch={this.props.handleKeywordSearch}
        />
        <ul className="navbar-nav px-3">
          {/* <SignInButton /> */}
        </ul>
      </Navbar>
    );
  }
}

export default Topbar;

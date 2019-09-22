import React from 'react';
import { Link } from "react-router-dom";

class HomePage extends React.Component {
  render() {
    return (
      <div>
        <div className="d-flex justify-content-between flex-wrap flex-md-nowrap align-items-center pt-3 pb-2 mb-3 border-bottom">
          <h1>Welcome to Find Open Data!</h1>
        </div>
        <p>
          This is a project to make <a href="https://en.wikipedia.org/wiki/Open_data">
          Open Data</a> searchable and accessible in one place.
          We have crawled over a million data files from various data portals
          and our crawl is constantly growing and being updated.
        </p>
        <p>
          Please start by trying out the search bar.
          Examples:
        </p>
        <ul>
          <li><Link to="/keyword-search?query=school%20reports%20new%20york%202017">school reports new york 2017</Link></li>
          <li><Link to="/keyword-search?query=building%20permits%20new%20york">building permits new york</Link></li>
          <li><Link to="/keyword-search?query=bike%20lanes%20seattle">bike lanes seattle</Link></li>
          <li><Link to="/keyword-search?query=nserc%20awards">nserc awards</Link></li>
        </ul>
        <p>
          For discussion please join us on <a href="https://groups.google.com/d/forum/findopendata">
            findopendata@googlegroups.com
          </a>.
        </p>
      </div>
    );
  }
}

export default HomePage;
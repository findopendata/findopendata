import React from 'react';
import { Nav } from 'react-bootstrap';
import SidebarPackageList from './SidebarPackageList'

class Sidebar extends React.Component {
  render() {
    return (
      <div className={this.props.className}>
        <Nav as="nav" className="bg-light">
          <div className="sidebar-sticky">
            <SidebarPackageList 
              name="Pinned Packages"
              packages={this.props.pinnedPackages} 
            />
            <SidebarPackageList
              name="Recent Packages"
              packages={this.props.recentPackages}
            />
          </div>
        </Nav>
      </div>
    );
  }
}

export default Sidebar;

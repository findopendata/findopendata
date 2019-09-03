import React from 'react';
import { OverlayTrigger, Nav, Tooltip } from 'react-bootstrap';
import { Link } from "react-router-dom";
import TextTruncate from 'react-text-truncate';

class SidebarPackageList extends React.Component {
  render() {
    return (
      <div className="sidebar-block">
        <h6 className="sidebar-heading d-flex justify-content-between align-items-center px-3 mt-4 mb-1 text-muted">
          {this.props.name}
        </h6>
        <Nav as="ul" className="flex-column mb-2">
          {
            this.props.packages.map((p) =>
              <Nav.Item key={p.id} as="li">
                <OverlayTrigger
                  placement="bottom"
                  overlay={
                    <Tooltip>
                      {p.title}
                    </Tooltip>
                  }
                >
                  <Link to={'/package/' + p.id} className="nav-link">
                    <TextTruncate
                      line={1}
                      truncateText="…"
                      text={p.title}
                    />
                  </Link>
                </OverlayTrigger>
              </Nav.Item>
            )
          }
        </Nav>
      </div>
    );
  }
}

export default SidebarPackageList;

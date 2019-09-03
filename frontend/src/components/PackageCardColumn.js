import React from 'react';
import {
  Card, Badge,
  ButtonToolbar, ButtonGroup, Button,
  Tooltip, OverlayTrigger,
} from 'react-bootstrap';
import TextTruncate from 'react-text-truncate';
import { Link } from "react-router-dom";

class PackageCard extends React.Component {
  constructor(props) {
    super(props);
    this.state = {
      isHovered: false
    };
    this.handleHover = this.handleHover.bind(this);
  }
  handleHover() {
    this.setState(prevState => ({
      isHovered: !prevState.isHovered,
    }));
  }
  render() {
    const p = this.props.package;
    const isHighlighted = this.props.isHighlighted ? this.props.isHighlighted(p) : false;
    const borderClass = this.state.isHovered ? "dark" : "";
    return (
      <Card 
        className="mb-4 package-card" 
        border={borderClass} 
        onMouseEnter={this.handleHover} 
        onMouseLeave={this.handleHover}
        key={p.id} 
        bg={isHighlighted ? 'dark' : ''} 
        text={isHighlighted ? 'white' : ''}
        >
        <Card.Header>{p.organization_display_name}</Card.Header>
        <Card.Body 
          onClick={() => {
            this.props.onClickPackageCard(p.id);
          }}>
          <Card.Title className="package-title">
            {p.title}
          </Card.Title>
          <Card.Subtitle className="mb-2">
            { p.modified ? p.modified : p.created }
          </Card.Subtitle>
          <TextTruncate
            line={6}
            truncateText="..."
            text={p.description}
            containerClassName="card-text"
          />
          <div className="d-flex flex-wrap">
            {
              p.tags.map(t => <Badge key={t} className="d-inline-flex m-1" variant="secondary">{t}</Badge>)
            }
          </div>
        </Card.Body>
        <Card.Footer className="">
          <div className="d-flex flex-row-reverse">
            <ButtonToolbar className="mb-2 mb-md-0">
              <ButtonGroup className="mr-2">
                <OverlayTrigger
                  placement="bottom"
                  trigger="hover"
                  overlay={
                    <Tooltip id={`tooltip-host-${p.id}`}>
                      Filter by This Source
                    </Tooltip>
                  }
                  >
                  <Button variant="outline-secondary" size="sm" className="host-name" 
                    onClick={(event) => {
                      event.preventDefault();
                      this.props.onClickSelectOriginalHost(p.original_host);
                    }}
                  >{p.original_host_display_name}</Button>
                </OverlayTrigger>
                <Link className="btn btn-outline-secondary btn-sm"
                  to={`/similar-packages?id=${p.id}`}
                >Find Similar</Link>
                <Button variant="outline-secondary" size="sm"
                  onClick={event => {
                    event.preventDefault();
                    this.props.onClickPin(p);
                  }}
                  active={this.props.isPinned(p)}
                  >Pin</Button>
              </ButtonGroup>
            </ButtonToolbar>
          </div>
        </Card.Footer>
      </Card>
    );
  }
}

class PackageCardColumn extends React.Component {
  render() {
    return(
      <div>
        {
          this.props.packages.map((p) => (
            <PackageCard 
              key={p.id}
              package={p}
              onClickPackageCard={this.props.onClickPackageCard}
              onClickPin={this.props.onClickPin}
              isPinned={this.props.isPinned}
              onClickSelectOriginalHost={this.props.onClickSelectOriginalHost}
              isHighlighted={this.props.isHighlighted}
            />
          ))
        }
      </div>
    );
  }
}

export default PackageCardColumn;
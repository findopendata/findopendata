import React from 'react';
import { Spinner } from "react-bootstrap";

export function LoadingSpinner(props) {
  return (
    <div className={props.loading ? 'processing-loading' : 'processing-done'}>
      <div className="d-flex justify-content-center my-10 py-10">
        <Spinner animation="border" role="status">
          <span className="sr-only">Loading...</span>
        </Spinner>
      </div>
    </div>
  );
}

export default LoadingSpinner;
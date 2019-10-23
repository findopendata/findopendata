import React from 'react';
import { withRouter } from "react-router-dom";
import Autosuggest from 'react-autosuggest';
import { FetchKeywordSearchTitleResults } from "../tools/RemoteData";
import Highlighter from "react-highlight-words";
import { debounce } from 'throttle-debounce';

class KeywordSearchForm extends React.Component {
  constructor(props) {
    super(props);
    let params = new URLSearchParams(this.props.location.search);
    const query = params.get("query");
    this.state = {
      value: query === null ? '' : query,
      suggestions: []
    };
    this.onSuggestionsFetchRequestedDebounced = debounce(500, this.onSuggestionsFetchRequested);
  }
  getSuggestionValue(suggestion) {
    return suggestion.title;
  }
  renderSuggestion(suggestion) {
    const tokens = this.state.value.split(' ');
    return (
      <div>
        <Highlighter
          highlightClassName="font-weight-bold"
          searchWords={tokens}
          autoEscape={true}
          textToHighlight={suggestion.title}
        /> -&nbsp;
        <span className="text-muted">
          {suggestion.organization_display_name}
        </span>
      </div>
    );
  }
  onChange = (event, { newValue }) => {
    this.setState({
      value: newValue
    });
  };
  onSuggestionsFetchRequested = ({ value }) => {
    FetchKeywordSearchTitleResults(value, [], (res) => {
          this.setState({
            suggestions: res,
          });
    });
  };
  onSuggestionsClearRequested = () => {
    this.setState({
      suggestions: []
    });
  };
  onKeyPress = (event) => {
    if (event.key === 'Enter') {
      // Fetch search results.
      this.props.handleKeywordSearch(this.state.value);
      // Route to result page.
      this.props.history.push('/keyword-search?query='+this.state.value);
    }
  }
  onSuggestionSelected = (event, {suggestion, suggestionValue, suggestionIndex, sectionIndex, method}) => {
    // Route to result page.
    this.props.history.push('/package/'+suggestion.id);
  };
  render() {
    const { value, suggestions } = this.state;
    const inputProps = {
      placeholder: 'Search',
      value,
      onChange: this.onChange,
      onKeyPress: this.onKeyPress,
    };
    const theme = {
      container: 'autocomplete w-100',
      input: 'form-control form-control-dark',
      suggestionsContainerOpen: 'suggestions-container bg-white text-dark',
      suggestionList: '',
      suggestion: 'p-2',
      suggestionHighlighted: 'bg-dark text-white',
    };
    return (
      <Autosuggest 
        theme={theme}
        suggestions={suggestions}
        onSuggestionsFetchRequested={this.onSuggestionsFetchRequestedDebounced}
        onSuggestionsClearRequested={this.onSuggestionsClearRequested}
        onSuggestionSelected={this.onSuggestionSelected}
        getSuggestionValue={this.getSuggestionValue.bind(this)}
        renderSuggestion={this.renderSuggestion.bind(this)}
        inputProps={inputProps}
      />
    );
  }
}

export default withRouter(KeywordSearchForm);


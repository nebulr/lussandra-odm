
function state(){

}

state.prototype.UNKNOWN = function() {
  return {
    value: 0,
    state_name: "Unknown"
  };
};

state.prototype.NEW = function() {
  return {
    value: 1,
    state_name: "New"
  };
};

state.prototype.UPDATE = function() {
  return {
    value: 2,
    state_name: "Update"
  };
};

state.prototype.DELETE = function() {
  return {
    value: 3,
    state_name: "Delete"
  };
};

state.prototype.equals = function (state1, state2) {
  return state1().state_name === state2().state_name;
}

module.exports = new state();

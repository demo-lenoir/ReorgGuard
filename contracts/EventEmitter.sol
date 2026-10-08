// SPDX-License-Identifier: Apache-2.0
pragma solidity ^0.8.20;

/// @notice Local Anvil fixture only. It stores no state and handles no funds.
contract EventEmitter {
    event Emitted(uint256 indexed value);

    function emitEvent(uint256 value) external {
        emit Emitted(value);
    }
}

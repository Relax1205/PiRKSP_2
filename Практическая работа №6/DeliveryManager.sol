// SPDX-License-Identifier: MIT
pragma solidity ^0.8.24;

contract DeliveryManager {
    enum Status { Created, Accepted, InTransit, Delivered, Cancelled }
    enum Role { None, Operator, Courier }

    struct Delivery {
        uint256 deliveryId;
        uint256 orderId;
        address customer;
        address courier;
        uint256 price; // в копейках
        Status status;
        uint256 createdAt;
        uint256 updatedAt;
    }

    address public immutable admin;
    uint256 public deliveriesCount;

    mapping(address => Role) public roles;
    mapping(uint256 => Delivery) private deliveries;
    mapping(uint256 => uint256) public deliveryIdByOrderId;

    event RoleGranted(address indexed account, Role role, address indexed by);
    event RoleRevoked(address indexed account, Role role, address indexed by);
    event DeliveryCreated(uint256 indexed deliveryId, uint256 indexed orderId, address indexed customer, uint256 price);
    event CourierAssigned(uint256 indexed deliveryId, address indexed courier, address indexed operator);
    event DeliveryStatusChanged(uint256 indexed deliveryId, Status from, Status to, address indexed by);

    error NotAdmin(address caller);
    error MissingRole(address caller, Role required);
    error NotAssignedCourier(address caller, address courier);
    error NotCustomerOrOperator(address caller);
    error DeliveryNotFound(uint256 deliveryId);
    error OrderAlreadyRegistered(uint256 orderId, uint256 deliveryId);
    error InvalidStatusTransition(Status from, Status to);
    error AccountIsNotCourier(address account);
    error ZeroAddress();
    error ZeroPrice();

    modifier onlyAdmin() {
        if (msg.sender != admin) revert NotAdmin(msg.sender);
        _;
    }

    modifier onlyRole(Role role) {
        if (roles[msg.sender] != role) revert MissingRole(msg.sender, role);
        _;
    }

    modifier deliveryExists(uint256 deliveryId) {
        if (deliveryId == 0 || deliveryId > deliveriesCount) revert DeliveryNotFound(deliveryId);
        _;
    }

    modifier allowedTransition(uint256 deliveryId, Status to) {
        Status from = deliveries[deliveryId].status;
        if (!canTransition(from, to)) revert InvalidStatusTransition(from, to);
        _;
    }

    modifier onlyAssignedCourier(uint256 deliveryId) {
        address courier = deliveries[deliveryId].courier;
        if (msg.sender != courier) revert NotAssignedCourier(msg.sender, courier);
        _;
    }

    constructor() {
        admin = msg.sender;
        roles[msg.sender] = Role.Operator;
        emit RoleGranted(msg.sender, Role.Operator, msg.sender);
    }

    function grantRole(address account, Role role) external onlyAdmin {
        if (account == address(0)) revert ZeroAddress();
        require(role != Role.None, "Use revokeRole to remove a role");
        roles[account] = role;
        emit RoleGranted(account, role, msg.sender);
    }

    function revokeRole(address account) external onlyAdmin {
        Role old = roles[account];
        roles[account] = Role.None;
        emit RoleRevoked(account, old, msg.sender);
    }

    function createDelivery(uint256 orderId, address customer, uint256 price)
        external
        onlyRole(Role.Operator)
        returns (uint256 deliveryId)
    {
        if (customer == address(0)) revert ZeroAddress();
        if (price == 0) revert ZeroPrice();
        uint256 existing = deliveryIdByOrderId[orderId];
        if (existing != 0) revert OrderAlreadyRegistered(orderId, existing);

        deliveryId = ++deliveriesCount;
        deliveries[deliveryId] = Delivery({
            deliveryId: deliveryId,
            orderId: orderId,
            customer: customer,
            courier: address(0),
            price: price,
            status: Status.Created,
            createdAt: block.timestamp,
            updatedAt: block.timestamp
        });
        deliveryIdByOrderId[orderId] = deliveryId;

        emit DeliveryCreated(deliveryId, orderId, customer, price);
    }

    function assignCourier(uint256 deliveryId, address courier)
        external
        onlyRole(Role.Operator)
        deliveryExists(deliveryId)
        allowedTransition(deliveryId, Status.Accepted)
    {
        if (roles[courier] != Role.Courier) revert AccountIsNotCourier(courier);

        deliveries[deliveryId].courier = courier;
        emit CourierAssigned(deliveryId, courier, msg.sender);
        _setStatus(deliveryId, Status.Accepted);
    }

    function startDelivery(uint256 deliveryId)
        external
        deliveryExists(deliveryId)
        allowedTransition(deliveryId, Status.InTransit)
        onlyAssignedCourier(deliveryId)
    {
        _setStatus(deliveryId, Status.InTransit);
    }

    function completeDelivery(uint256 deliveryId)
        external
        deliveryExists(deliveryId)
        allowedTransition(deliveryId, Status.Delivered)
        onlyAssignedCourier(deliveryId)
    {
        _setStatus(deliveryId, Status.Delivered);
    }

    function cancelDelivery(uint256 deliveryId)
        external
        deliveryExists(deliveryId)
        allowedTransition(deliveryId, Status.Cancelled)
    {
        if (msg.sender != deliveries[deliveryId].customer && roles[msg.sender] != Role.Operator) {
            revert NotCustomerOrOperator(msg.sender);
        }
        _setStatus(deliveryId, Status.Cancelled);
    }

    function getDelivery(uint256 deliveryId) external view deliveryExists(deliveryId) returns (Delivery memory) {
        return deliveries[deliveryId];
    }

    function statusName(uint256 deliveryId) external view deliveryExists(deliveryId) returns (string memory) {
        Status s = deliveries[deliveryId].status;
        if (s == Status.Created) return "Created";
        if (s == Status.Accepted) return "Accepted";
        if (s == Status.InTransit) return "InTransit";
        if (s == Status.Delivered) return "Delivered";
        return "Cancelled";
    }

    // после InTransit отменить уже нельзя
    function canTransition(Status from, Status to) public pure returns (bool) {
        if (from == Status.Created) return to == Status.Accepted || to == Status.Cancelled;
        if (from == Status.Accepted) return to == Status.InTransit || to == Status.Cancelled;
        if (from == Status.InTransit) return to == Status.Delivered;
        return false;
    }

    function _setStatus(uint256 deliveryId, Status to) private {
        Delivery storage d = deliveries[deliveryId];
        Status from = d.status;
        d.status = to;
        d.updatedAt = block.timestamp;
        emit DeliveryStatusChanged(deliveryId, from, to, msg.sender);
    }
}

# blvm-index

Script history, Electrum, and Esplora. The database format is unchanged.

The node binds the listeners. This process does not. Electrum is `[services.electrum]` and Esplora is `[services.esplora]`. An unset `listen` does not bind.

`GET /fee-estimates` and `GET /api/fee-estimates` return target keys mapped to sat/vB. The quotes come from the node's combined conservative fee table.

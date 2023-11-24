# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: graphsenselib/ingest/tron/grpc/core/common.proto
"""Generated protocol buffer code."""
from google.protobuf.internal import enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n0graphsenselib/ingest/tron/grpc/core/common.proto\x12\x08protocol\"*\n\tAccountId\x12\x0c\n\x04name\x18\x01 \x01(\x0c\x12\x0f\n\x07\x61\x64\x64ress\x18\x02 \x01(\x0c\"J\n\tauthority\x12$\n\x07\x61\x63\x63ount\x18\x01 \x01(\x0b\x32\x13.protocol.AccountId\x12\x17\n\x0fpermission_name\x18\x02 \x01(\x0c\"&\n\x03Key\x12\x0f\n\x07\x61\x64\x64ress\x18\x01 \x01(\x0c\x12\x0e\n\x06weight\x18\x02 \x01(\x03\"\xf1\x01\n\nPermission\x12\x31\n\x04type\x18\x01 \x01(\x0e\x32#.protocol.Permission.PermissionType\x12\n\n\x02id\x18\x02 \x01(\x05\x12\x17\n\x0fpermission_name\x18\x03 \x01(\t\x12\x11\n\tthreshold\x18\x04 \x01(\x03\x12\x11\n\tparent_id\x18\x05 \x01(\x05\x12\x12\n\noperations\x18\x06 \x01(\x0c\x12\x1b\n\x04keys\x18\x07 \x03(\x0b\x32\r.protocol.Key\"4\n\x0ePermissionType\x12\t\n\x05Owner\x10\x00\x12\x0b\n\x07Witness\x10\x01\x12\n\n\x06\x41\x63tive\x10\x02\"\x83\x07\n\rSmartContract\x12\x16\n\x0eorigin_address\x18\x01 \x01(\x0c\x12\x18\n\x10\x63ontract_address\x18\x02 \x01(\x0c\x12(\n\x03\x61\x62i\x18\x03 \x01(\x0b\x32\x1b.protocol.SmartContract.ABI\x12\x10\n\x08\x62ytecode\x18\x04 \x01(\x0c\x12\x12\n\ncall_value\x18\x05 \x01(\x03\x12%\n\x1d\x63onsume_user_resource_percent\x18\x06 \x01(\x03\x12\x0c\n\x04name\x18\x07 \x01(\t\x12\x1b\n\x13origin_energy_limit\x18\x08 \x01(\x03\x12\x11\n\tcode_hash\x18\t \x01(\x0c\x12\x10\n\x08trx_hash\x18\n \x01(\x0c\x1a\xf8\x04\n\x03\x41\x42I\x12\x31\n\x06\x65ntrys\x18\x01 \x03(\x0b\x32!.protocol.SmartContract.ABI.Entry\x1a\xbd\x04\n\x05\x45ntry\x12\x11\n\tanonymous\x18\x01 \x01(\x08\x12\x10\n\x08\x63onstant\x18\x02 \x01(\x08\x12\x0c\n\x04name\x18\x03 \x01(\t\x12\x37\n\x06inputs\x18\x04 \x03(\x0b\x32\'.protocol.SmartContract.ABI.Entry.Param\x12\x38\n\x07outputs\x18\x05 \x03(\x0b\x32\'.protocol.SmartContract.ABI.Entry.Param\x12\x39\n\x04type\x18\x06 \x01(\x0e\x32+.protocol.SmartContract.ABI.Entry.EntryType\x12\x0f\n\x07payable\x18\x07 \x01(\x08\x12N\n\x0fstateMutability\x18\x08 \x01(\x0e\x32\x35.protocol.SmartContract.ABI.Entry.StateMutabilityType\x1a\x34\n\x05Param\x12\x0f\n\x07indexed\x18\x01 \x01(\x08\x12\x0c\n\x04name\x18\x02 \x01(\t\x12\x0c\n\x04type\x18\x03 \x01(\t\"Y\n\tEntryType\x12\x14\n\x10UnknownEntryType\x10\x00\x12\x0f\n\x0b\x43onstructor\x10\x01\x12\x0c\n\x08\x46unction\x10\x02\x12\t\n\x05\x45vent\x10\x03\x12\x0c\n\x08\x46\x61llback\x10\x04\"a\n\x13StateMutabilityType\x12\x19\n\x15UnknownMutabilityType\x10\x00\x12\x08\n\x04Pure\x10\x01\x12\x08\n\x04View\x10\x02\x12\x0e\n\nNonpayable\x10\x03\x12\x0b\n\x07Payable\x10\x04\"0\n\x04Vote\x12\x14\n\x0cvote_address\x18\x01 \x01(\x0c\x12\x12\n\nvote_count\x18\x02 \x01(\x03\"I\n\x04Note\x12\r\n\x05value\x18\x01 \x01(\x03\x12\x17\n\x0fpayment_address\x18\x02 \x01(\t\x12\x0b\n\x03rcm\x18\x03 \x01(\x0c\x12\x0c\n\x04memo\x18\x04 \x01(\x0c*)\n\x0cResourceCode\x12\r\n\tBANDWIDTH\x10\x00\x12\n\n\x06\x45NERGY\x10\x01*7\n\x0b\x41\x63\x63ountType\x12\n\n\x06Normal\x10\x00\x12\x0e\n\nAssetIssue\x10\x01\x12\x0c\n\x08\x43ontract\x10\x02\x62\x06proto3')

_RESOURCECODE = DESCRIPTOR.enum_types_by_name['ResourceCode']
ResourceCode = enum_type_wrapper.EnumTypeWrapper(_RESOURCECODE)
_ACCOUNTTYPE = DESCRIPTOR.enum_types_by_name['AccountType']
AccountType = enum_type_wrapper.EnumTypeWrapper(_ACCOUNTTYPE)
BANDWIDTH = 0
ENERGY = 1
Normal = 0
AssetIssue = 1
Contract = 2


_ACCOUNTID = DESCRIPTOR.message_types_by_name['AccountId']
_AUTHORITY = DESCRIPTOR.message_types_by_name['authority']
_KEY = DESCRIPTOR.message_types_by_name['Key']
_PERMISSION = DESCRIPTOR.message_types_by_name['Permission']
_SMARTCONTRACT = DESCRIPTOR.message_types_by_name['SmartContract']
_SMARTCONTRACT_ABI = _SMARTCONTRACT.nested_types_by_name['ABI']
_SMARTCONTRACT_ABI_ENTRY = _SMARTCONTRACT_ABI.nested_types_by_name['Entry']
_SMARTCONTRACT_ABI_ENTRY_PARAM = _SMARTCONTRACT_ABI_ENTRY.nested_types_by_name['Param']
_VOTE = DESCRIPTOR.message_types_by_name['Vote']
_NOTE = DESCRIPTOR.message_types_by_name['Note']
_PERMISSION_PERMISSIONTYPE = _PERMISSION.enum_types_by_name['PermissionType']
_SMARTCONTRACT_ABI_ENTRY_ENTRYTYPE = _SMARTCONTRACT_ABI_ENTRY.enum_types_by_name['EntryType']
_SMARTCONTRACT_ABI_ENTRY_STATEMUTABILITYTYPE = _SMARTCONTRACT_ABI_ENTRY.enum_types_by_name['StateMutabilityType']
AccountId = _reflection.GeneratedProtocolMessageType('AccountId', (_message.Message,), {
  'DESCRIPTOR' : _ACCOUNTID,
  '__module__' : 'graphsenselib.ingest.tron.grpc.core.common_pb2'
  # @@protoc_insertion_point(class_scope:protocol.AccountId)
  })
_sym_db.RegisterMessage(AccountId)

authority = _reflection.GeneratedProtocolMessageType('authority', (_message.Message,), {
  'DESCRIPTOR' : _AUTHORITY,
  '__module__' : 'graphsenselib.ingest.tron.grpc.core.common_pb2'
  # @@protoc_insertion_point(class_scope:protocol.authority)
  })
_sym_db.RegisterMessage(authority)

Key = _reflection.GeneratedProtocolMessageType('Key', (_message.Message,), {
  'DESCRIPTOR' : _KEY,
  '__module__' : 'graphsenselib.ingest.tron.grpc.core.common_pb2'
  # @@protoc_insertion_point(class_scope:protocol.Key)
  })
_sym_db.RegisterMessage(Key)

Permission = _reflection.GeneratedProtocolMessageType('Permission', (_message.Message,), {
  'DESCRIPTOR' : _PERMISSION,
  '__module__' : 'graphsenselib.ingest.tron.grpc.core.common_pb2'
  # @@protoc_insertion_point(class_scope:protocol.Permission)
  })
_sym_db.RegisterMessage(Permission)

SmartContract = _reflection.GeneratedProtocolMessageType('SmartContract', (_message.Message,), {

  'ABI' : _reflection.GeneratedProtocolMessageType('ABI', (_message.Message,), {

    'Entry' : _reflection.GeneratedProtocolMessageType('Entry', (_message.Message,), {

      'Param' : _reflection.GeneratedProtocolMessageType('Param', (_message.Message,), {
        'DESCRIPTOR' : _SMARTCONTRACT_ABI_ENTRY_PARAM,
        '__module__' : 'graphsenselib.ingest.tron.grpc.core.common_pb2'
        # @@protoc_insertion_point(class_scope:protocol.SmartContract.ABI.Entry.Param)
        })
      ,
      'DESCRIPTOR' : _SMARTCONTRACT_ABI_ENTRY,
      '__module__' : 'graphsenselib.ingest.tron.grpc.core.common_pb2'
      # @@protoc_insertion_point(class_scope:protocol.SmartContract.ABI.Entry)
      })
    ,
    'DESCRIPTOR' : _SMARTCONTRACT_ABI,
    '__module__' : 'graphsenselib.ingest.tron.grpc.core.common_pb2'
    # @@protoc_insertion_point(class_scope:protocol.SmartContract.ABI)
    })
  ,
  'DESCRIPTOR' : _SMARTCONTRACT,
  '__module__' : 'graphsenselib.ingest.tron.grpc.core.common_pb2'
  # @@protoc_insertion_point(class_scope:protocol.SmartContract)
  })
_sym_db.RegisterMessage(SmartContract)
_sym_db.RegisterMessage(SmartContract.ABI)
_sym_db.RegisterMessage(SmartContract.ABI.Entry)
_sym_db.RegisterMessage(SmartContract.ABI.Entry.Param)

Vote = _reflection.GeneratedProtocolMessageType('Vote', (_message.Message,), {
  'DESCRIPTOR' : _VOTE,
  '__module__' : 'graphsenselib.ingest.tron.grpc.core.common_pb2'
  # @@protoc_insertion_point(class_scope:protocol.Vote)
  })
_sym_db.RegisterMessage(Vote)

Note = _reflection.GeneratedProtocolMessageType('Note', (_message.Message,), {
  'DESCRIPTOR' : _NOTE,
  '__module__' : 'graphsenselib.ingest.tron.grpc.core.common_pb2'
  # @@protoc_insertion_point(class_scope:protocol.Note)
  })
_sym_db.RegisterMessage(Note)

if _descriptor._USE_C_DESCRIPTORS == False:

  DESCRIPTOR._options = None
  _RESOURCECODE._serialized_start=1493
  _RESOURCECODE._serialized_end=1534
  _ACCOUNTTYPE._serialized_start=1536
  _ACCOUNTTYPE._serialized_end=1591
  _ACCOUNTID._serialized_start=62
  _ACCOUNTID._serialized_end=104
  _AUTHORITY._serialized_start=106
  _AUTHORITY._serialized_end=180
  _KEY._serialized_start=182
  _KEY._serialized_end=220
  _PERMISSION._serialized_start=223
  _PERMISSION._serialized_end=464
  _PERMISSION_PERMISSIONTYPE._serialized_start=412
  _PERMISSION_PERMISSIONTYPE._serialized_end=464
  _SMARTCONTRACT._serialized_start=467
  _SMARTCONTRACT._serialized_end=1366
  _SMARTCONTRACT_ABI._serialized_start=734
  _SMARTCONTRACT_ABI._serialized_end=1366
  _SMARTCONTRACT_ABI_ENTRY._serialized_start=793
  _SMARTCONTRACT_ABI_ENTRY._serialized_end=1366
  _SMARTCONTRACT_ABI_ENTRY_PARAM._serialized_start=1124
  _SMARTCONTRACT_ABI_ENTRY_PARAM._serialized_end=1176
  _SMARTCONTRACT_ABI_ENTRY_ENTRYTYPE._serialized_start=1178
  _SMARTCONTRACT_ABI_ENTRY_ENTRYTYPE._serialized_end=1267
  _SMARTCONTRACT_ABI_ENTRY_STATEMUTABILITYTYPE._serialized_start=1269
  _SMARTCONTRACT_ABI_ENTRY_STATEMUTABILITYTYPE._serialized_end=1366
  _VOTE._serialized_start=1368
  _VOTE._serialized_end=1416
  _NOTE._serialized_start=1418
  _NOTE._serialized_end=1491
# @@protoc_insertion_point(module_scope)
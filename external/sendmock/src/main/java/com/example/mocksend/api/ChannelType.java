package com.example.mocksend.api;

import com.fasterxml.jackson.annotation.JsonEnumDefaultValue;

public enum ChannelType {
    KAKAO,
    SMS,
    EMAIL,
    @JsonEnumDefaultValue
    UNKNOWN
}

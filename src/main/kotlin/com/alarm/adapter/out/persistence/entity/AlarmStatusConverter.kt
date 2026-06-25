package com.alarm.adapter.out.persistence.entity

import com.alarm.domain.AlarmStatus
import jakarta.persistence.AttributeConverter
import jakarta.persistence.Converter

/**
 * [파일명] AlarmStatusConverter.kt
 * [기능] AlarmStatus 열거형을 DB의 SMALLINT(Short)로, DB의 값을 다시 열거형으로 변환합니다.
 */
@Converter(autoApply = true)
class AlarmStatusConverter : AttributeConverter<AlarmStatus, Short> {

    override fun convertToDatabaseColumn(attribute: AlarmStatus?): Short? {
        return attribute?.code
    }

    override fun convertToEntityAttribute(dbData: Short?): AlarmStatus? {
        if (dbData == null) return null
        return AlarmStatus.fromCode(dbData)
    }
}

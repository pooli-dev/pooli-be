package com.pooli.application.mapper;

import com.pooli.application.domain.dto.response.AppResDto;
import com.pooli.application.domain.entity.ApplicationCategory;
import java.util.List;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

@Mapper
public interface AppMapper {
    List<AppResDto> findApps(
        @Param("category") ApplicationCategory category,
        @Param("keyword") String keyword
    );
}

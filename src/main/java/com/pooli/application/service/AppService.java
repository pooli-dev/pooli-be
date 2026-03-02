package com.pooli.application.service;

import com.pooli.application.domain.dto.response.AppResDto;
import com.pooli.application.domain.entity.ApplicationCategory;
import java.util.List;

public interface AppService {
    List<AppResDto> getApps(ApplicationCategory category, String keyword);
}

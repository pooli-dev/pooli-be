package com.pooli.application.service;

import com.pooli.application.domain.dto.response.AppResDto;
import com.pooli.application.domain.entity.ApplicationCategory;
import com.pooli.application.mapper.AppMapper;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class AppServiceImpl implements AppService {

    private final AppMapper appMapper;

    @Override
    public List<AppResDto> getApps(ApplicationCategory category, String keyword) {
        return appMapper.findApps(category, keyword);
    }
}

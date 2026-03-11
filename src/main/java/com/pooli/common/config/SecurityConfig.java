package com.pooli.common.config;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpMethod;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.config.annotation.authentication.configuration.AuthenticationConfiguration;
import org.springframework.security.config.annotation.method.configuration.EnableMethodSecurity;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.csrf.CsrfTokenRepository;
import org.springframework.web.cors.CorsConfigurationSource;

import com.pooli.auth.exception.CustomAccessDeniedHandler;
import com.pooli.auth.exception.CustomAuthenticationEntryPoint;

@EnableMethodSecurity
@Configuration
@Profile("!traffic")
public class SecurityConfig {
    /**
     * Configure and build the application's SecurityFilterChain with the project's security rules.
     *
     * Configures CORS, disables HTTP Basic and form login, sets session creation policy to IF_REQUIRED,
     * installs custom authentication and access-denied handlers, permits public access to documentation,
     * auth endpoints, traffic requests, error and actuator paths, allows all OPTIONS requests, requires
     * authentication for other requests, and delegates CSRF customization to the provided customizer.
     *
     * @param http the HttpSecurity builder supplied by Spring Security
     * @param corsConfigurationSource the CORS configuration source to apply
     * @param csrfTokenRepository repository used to persist CSRF tokens
     * @param csrfCustomizer component that applies additional CSRF configuration to HttpSecurity
     * @param customAuthenticationEntryPoint handler invoked for unauthenticated requests (401)
     * @param customAccessDeniedHandler handler invoked for access-denied events (403)
     * @return the configured SecurityFilterChain
     * @throws Exception if an error occurs while configuring or building the security filter chain
     */
    @Bean
    public SecurityFilterChain securityFilterChain(
        HttpSecurity http,
        @Qualifier("corsConfigurationSource") CorsConfigurationSource corsConfigurationSource,
        CsrfTokenRepository csrfTokenRepository,
        CsrfCustomizer csrfCustomizer,
        CustomAuthenticationEntryPoint customAuthenticationEntryPoint,
        CustomAccessDeniedHandler customAccessDeniedHandler
    ) throws Exception {
        http
            .cors(cors -> cors.configurationSource(corsConfigurationSource))
            .httpBasic(httpBasic -> httpBasic.disable())
            .formLogin(form -> form.disable())
            .sessionManagement(session -> session.sessionCreationPolicy(SessionCreationPolicy.IF_REQUIRED))
            .exceptionHandling(ex -> ex
                .authenticationEntryPoint(customAuthenticationEntryPoint) // 미로그인(401)
                .accessDeniedHandler(customAccessDeniedHandler) // 권한 체크(403)
            )
            .authorizeHttpRequests(auth -> auth
                .requestMatchers(
                    "/swagger-ui.html",
                    "/swagger-ui/**",
                    "/v3/api-docs/**",
                    "/api/auth/admin/login",
                    "/api/auth/user/login",
                    "/api/auth/logout",
                    "/api/traffic/requests",
                    "/error",
                    "/actuator/**"
                ).permitAll()
                .requestMatchers(HttpMethod.OPTIONS, "/**").permitAll()
                .anyRequest().authenticated()
            );

        csrfCustomizer.customize(http, csrfTokenRepository);

        return http.build();
    }

    @Bean
    public PasswordEncoder passwordEncoder() {
        return new BCryptPasswordEncoder();
    }

    @Bean
    public AuthenticationManager authenticationManager(AuthenticationConfiguration configuration) throws Exception {
        return configuration.getAuthenticationManager();
    }
}

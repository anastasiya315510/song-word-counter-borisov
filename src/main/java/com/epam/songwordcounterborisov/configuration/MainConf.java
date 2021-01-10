package com.epam.songwordcounterborisov.configuration;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.ActiveProfiles;

import java.util.List;

import static com.epam.songwordcounterborisov.configuration.SparkConfig.DEV;

@Configuration
@ComponentScan("com.epam.songwordcounterborisov")
public class MainConf {

}

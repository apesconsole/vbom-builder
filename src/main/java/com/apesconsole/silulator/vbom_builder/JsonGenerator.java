package com.apesconsole.silulator.vbom_builder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.springframework.stereotype.Component;

@Component
public class JsonGenerator {
	
	public String readFile(String filePath) throws IOException {
        return new String(Files.readAllBytes(Paths.get(filePath)));
    }
	
}

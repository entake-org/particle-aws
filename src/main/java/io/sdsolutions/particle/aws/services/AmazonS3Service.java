package io.sdsolutions.particle.aws.services;

import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;

public interface AmazonS3Service {

    byte[] getDocumentFromS3(String documentKey) throws IOException;

    void uploadDocumentToS3(MultipartFile file, String documentKey, boolean isPublic) throws IOException;

    void deleteDocumentFromS3(String documentKey);
}

package org.movealong.plugins.lambda;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.lambda.AWSLambda;
import com.amazonaws.services.lambda.AWSLambdaClientBuilder;
import com.amazonaws.services.lambda.model.FunctionConfiguration;
import com.amazonaws.services.lambda.model.UpdateFunctionCodeRequest;
import com.amazonaws.services.lambda.model.UpdateFunctionCodeResult;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import com.jnape.palatable.lambda.adt.Either;
import com.jnape.palatable.lambda.adt.hlist.Tuple2;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugin.logging.Log;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.project.MavenProject;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import static com.jnape.palatable.lambda.adt.Either.*;
import static com.jnape.palatable.lambda.adt.hlist.HList.tuple;
import static com.jnape.palatable.lambda.functions.builtin.fn2.Find.find;
import static java.lang.String.format;

@Mojo(name = "deploy")
@Data
@EqualsAndHashCode(callSuper = false)
public class LambdaDeployMojo extends AbstractMojo {

    @Parameter(defaultValue = "${project}", readonly = true, required = true)
    private MavenProject project;

    @Parameter(required = true)
    private String region;

    @Parameter(required = true)
    private String functionArn;

    @Parameter(required = true)
    private File codeBundle;

    @Parameter(required = true)
    private String s3Bucket;

    @Parameter(required = true)
    private String s3Prefix;

    @Override
    public void execute() throws MojoFailureException {
        getLog().info(format("Successfully updated function %s",
                             Either.<String, File>right(getCodeBundle())
                                     .flatMap(fileExists())
                                     .flatMap(functionExists(getRegion(), getFunctionArn()))
                                     .flatMap(uploadToS3(getLog(), getRegion(), getS3Bucket(), getS3Prefix()))
                                     .flatMap(updateFunctionCode(getRegion(), getS3Bucket(), getS3Prefix()))
                                     .fmap(UpdateFunctionCodeResult::getFunctionArn)
                                     .orThrow(MojoFailureException::new)));
    }

    private static Function<Tuple2<File, FunctionConfiguration>, Either<String, UpdateFunctionCodeResult>> updateFunctionCode(String region, String bucket, String prefix) {
        return t -> trying(() -> {
            FunctionConfiguration config = t._2();
            String keyName = prefix + "/" + t._1().getName();
            return withLambdaClient(
                    region,
                    lambda -> lambda.updateFunctionCode(new UpdateFunctionCodeRequest()
                                                             .withFunctionName(config.getFunctionName())
                                                             .withS3Bucket(bucket)
                                                             .withS3Key(keyName)
                                                             .withPublish(false)));
        }).biMapL(Throwable::getMessage);
    }

    private static <T> Function<T, Either<String, Tuple2<T, FunctionConfiguration>>> functionExists(String region, String functionArn) {
        return (T t) -> withLambdaClient(
                region,
                lambda ->
                        find(f -> f.getFunctionArn().equals(functionArn),
                             lambda.listFunctions().getFunctions())
                                .fmap(f -> tuple(t, f))
                                .toEither(() -> format("Function not found: %s", functionArn)));
    }

    private static Function<Tuple2<File, FunctionConfiguration>, Either<String, Tuple2<File, FunctionConfiguration>>> uploadToS3(Log log, String region, String bucket, String prefix) {
        return t -> {
            File f = t._1();
            return withS3Client(
                    region,
                    s3Client -> {
                        String keyName = prefix + "/" + f.getName();
                        log.info(format("Uploading %s to s3://%s/%s", f.getName(), bucket, keyName));
                        long contentLength = f.length();

                        return trying(() -> {
                            long partSize = 5 * 1024 * 1024; // Set part size to 5 MB.

                            // Create a list of ETag objects. You retrieve ETags for each object part uploaded,
                            // then, after each individual part has been uploaded, pass the list of ETags to
                            // the request to complete the upload.
                            List<PartETag> partETags = new ArrayList<>();

                            // Initiate the multipart upload.
                            InitiateMultipartUploadRequest initRequest  = new InitiateMultipartUploadRequest(bucket, keyName);
                            InitiateMultipartUploadResult  initResponse = s3Client.initiateMultipartUpload(initRequest);

                            // Upload the file parts.
                            long filePosition = 0;
                            for (int i = 1; filePosition < contentLength; i++) {
                                // Because the last part could be less than 5 MB, adjust the part size as needed.
                                partSize = Math.min(partSize, (contentLength - filePosition));

                                // Create the request to upload a part.
                                UploadPartRequest uploadRequest = new UploadPartRequest()
                                        .withBucketName(bucket)
                                        .withKey(keyName)
                                        .withUploadId(initResponse.getUploadId())
                                        .withPartNumber(i)
                                        .withFileOffset(filePosition)
                                        .withFile(f)
                                        .withPartSize(partSize);

                                // Upload the part and add the response's ETag to our list.
                                UploadPartResult uploadResult = s3Client.uploadPart(uploadRequest);
                                partETags.add(uploadResult.getPartETag());

                                filePosition += partSize;
                            }

                            // Complete the multipart upload.
                            s3Client.completeMultipartUpload(new CompleteMultipartUploadRequest(bucket, keyName,
                                                                                                initResponse.getUploadId(), partETags));
                            return t;
                        }).biMapL(Throwable::getMessage);
                    });
        };
    }

    private static Function<File, Either<String, File>> fileExists() {
        return f -> f.exists() ? right(f)
                               : left(format("Code bundle not found: %s", f.getAbsolutePath()));
    }

    private static <T> T withLambdaClient(String region, Function<AWSLambda, T> fn) {
        AWSLambda lambdaClient = AWSLambdaClientBuilder
                .standard()
                .withCredentials(DefaultAWSCredentialsProviderChain.getInstance())
                .withRegion(region)
                .build();
        try {
            return fn.apply(lambdaClient);
        } finally {
            lambdaClient.shutdown();
        }
    }

    private static <T> T withS3Client(String region, Function<AmazonS3, T> fn) {
        AmazonS3 s3Client = AmazonS3ClientBuilder
                .standard()
                .withCredentials(DefaultAWSCredentialsProviderChain.getInstance())
                .withRegion(region)
                .build();
        try {
            return fn.apply(s3Client);
        } finally {
            s3Client.shutdown();
        }
    }
}

package org.movealong.plugins.lambda;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.lambda.AWSLambda;
import com.amazonaws.services.lambda.AWSLambdaClientBuilder;
import com.amazonaws.services.lambda.model.UpdateFunctionCodeRequest;
import com.amazonaws.services.lambda.model.UpdateFunctionCodeResult;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import com.jnape.palatable.lambda.adt.Either;
import com.jnape.palatable.lambda.adt.hlist.Tuple2;
import com.jnape.palatable.lambda.adt.hlist.Tuple3;
import com.jnape.palatable.lambda.functions.builtin.fn1.Id;
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
import java.util.function.Function;

import static com.jnape.palatable.lambda.adt.Either.*;
import static com.jnape.palatable.lambda.adt.hlist.HList.tuple;
import static com.jnape.palatable.lambda.functions.builtin.fn2.Find.find;
import static com.jnape.palatable.lambda.functions.builtin.fn2.Map.map;
import static com.jnape.palatable.lambda.functions.builtin.fn2.ToCollection.toCollection;
import static java.lang.String.format;
import static org.movealong.plugins.lambda.PartNumber.partNumbers;

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
                             fileExists(getCodeBundle())
                                     .flatMap(functionExists(getRegion(), getFunctionArn()))
                                     .flatMap(uploadBundle(getLog(), getRegion(), getS3Bucket(), getS3Prefix()))
                                     .flatMap(updateFunctionCode(getRegion(), getS3Bucket()))
                                     .fmap(UpdateFunctionCodeResult::getFunctionArn)
                                     .orThrow(MojoFailureException::new)));
    }

    private static Either<String, File> fileExists(File codeBundle) {
        return codeBundle.exists() ? right(codeBundle)
                                   : left(format("Code bundle not found: %s", codeBundle.getAbsolutePath()));
    }

    private static <T> Function<T, Either<String, Tuple2<T, String>>>
    functionExists(String region, String functionArn) {
        return (T t) -> withLambdaClient(
                region,
                client -> find(f -> f.getFunctionArn().equals(functionArn), client.listFunctions().getFunctions())
                        .fmap(f -> tuple(t, f.getFunctionName()))
                        .toEither(() -> format("Function not found: %s", functionArn)));
    }

    private static Function<Tuple2<File, String>, Either<String, Tuple3<String, String, CompleteMultipartUploadResult>>>
    uploadBundle(Log log, String region, String bucket, String prefix) {
        return t -> withS3Client(region, client -> trying(
                () -> {
                    File   f       = t._1();
                    String keyName = prefix + "/" + f.getName();
                    log.info(format("Uploading: s3://%s/%s", bucket, keyName));

                    String uploadId =
                            client.initiateMultipartUpload(new InitiateMultipartUploadRequest(bucket, keyName))
                                  .getUploadId();
                    return tuple(keyName, t._2(), client.completeMultipartUpload(
                            new CompleteMultipartUploadRequest(bucket, keyName, uploadId, Id.<Iterable<PartNumber>>id()
                                    .fmap(map(createUploadPartRequest(bucket, f, keyName, uploadId)))
                                    .fmap(map(client::uploadPart))
                                    .fmap(map(UploadPartResult::getPartETag))
                                    .fmap(toCollection(ArrayList::new))
                                    .apply(partNumbers(f.length())))));
                })
                .biMapL(Throwable::getMessage));
    }

    private static Function<PartNumber, UploadPartRequest> createUploadPartRequest(String bucket, File f, String keyName, String uploadId) {
        return partNumber -> new UploadPartRequest()
                .withBucketName(bucket)
                .withKey(keyName)
                .withUploadId(uploadId)
                .withPartNumber(partNumber.getPartNumber())
                .withFileOffset(partNumber.getStartPosition())
                .withPartSize(partNumber.getPartSize())
                .withFile(f);
    }

    private static Function<Tuple3<String, String, CompleteMultipartUploadResult>, Either<String, UpdateFunctionCodeResult>>
    updateFunctionCode(String region, String bucket) {
        return t -> trying(() -> withLambdaClient(
                region,
                lambda -> lambda.updateFunctionCode(
                        new UpdateFunctionCodeRequest()
                                .withFunctionName(t._2())
                                .withS3Bucket(bucket)
                                .withS3Key(t._1())
                                .withPublish(false))))
                .biMapL(Throwable::getMessage);
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

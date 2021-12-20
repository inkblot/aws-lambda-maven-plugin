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
import com.jnape.palatable.lambda.adt.Maybe;
import com.jnape.palatable.lambda.adt.hlist.Tuple2;
import com.jnape.palatable.lambda.adt.hlist.Tuple3;
import com.jnape.palatable.lambda.functions.Fn1;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugin.logging.Log;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;

import java.io.File;
import java.util.ArrayList;

import static com.jnape.palatable.lambda.adt.Either.trying;
import static com.jnape.palatable.lambda.adt.Maybe.just;
import static com.jnape.palatable.lambda.adt.Maybe.maybe;
import static com.jnape.palatable.lambda.adt.hlist.HList.tuple;
import static com.jnape.palatable.lambda.functions.builtin.fn1.Constantly.constantly;
import static com.jnape.palatable.lambda.functions.builtin.fn2.Find.find;
import static com.jnape.palatable.lambda.functions.builtin.fn2.Map.map;
import static com.jnape.palatable.lambda.functions.builtin.fn2.ToCollection.toCollection;
import static com.jnape.palatable.lambda.functions.builtin.fn2.Tupler2.tupler;
import static java.lang.String.format;
import static org.movealong.plugins.lambda.PartNumber.partNumbers;

@Mojo(name = "deploy")
@Data
@EqualsAndHashCode(callSuper = false)
public class LambdaDeployMojo extends AbstractMojo {

    @Parameter(required = true)
    private String region;

    @Parameter
    private String functionArn;

    @Parameter(required = true)
    private File codeBundle;

    @Parameter(required = true)
    private String s3Bucket;

    @Parameter(required = true)
    private String s3Prefix;

    @Parameter(defaultValue = "false")
    private boolean publish;

    @Override
    public void execute() throws MojoFailureException {
        getLog().info(format("Successfully updated function %s",
                             fileExists(getCodeBundle())
                                     .flatMap(functionExists(getRegion(), maybe(getFunctionArn())))
                                     .flatMap(uploadBundle(getLog(), getRegion(), getS3Bucket(), getS3Prefix()))
                                     .flatMap(updateFunctionCode(getRegion(), getS3Bucket(), isPublish()))
                                     .fmap(m -> m.match(constantly("N/A"),
                                                        UpdateFunctionCodeResult::getFunctionArn))
                                     .orThrow(MojoFailureException::new)));
    }

    private static Either<String, File> fileExists(File codeBundle) {
        return just(codeBundle).filter(File::exists)
                               .toEither(() -> format("Code bundle not found: %s", codeBundle.getAbsolutePath()));
    }

    private static <T> Fn1<T, Either<String, Tuple2<T, Maybe<String>>>>
    functionExists(String region, Maybe<String> mFunctionArn) {
        return (T t) -> withLambdaClient(
                region,
                client -> mFunctionArn
                        .<String, Either<String, ?>, Maybe<String>, Either<String, Maybe<String>>>traverse(
                                functionArn -> find(functionConfig -> functionConfig.getFunctionArn().equals(functionArn),
                                                    client.listFunctions().getFunctions())
                                        .fmap(FunctionConfiguration::getFunctionName)
                                        .toEither(() -> format("Function not found: %s", functionArn)),
                                Either::right))
                .fmap(tupler(t));
    }

    private static <T> Fn1<Tuple2<File, T>, Either<String, Tuple3<String, T, CompleteMultipartUploadResult>>>
    uploadBundle(Log log, String region, String bucket, String prefix) {
        return t -> t.into((codeBundle, functionName) -> withS3Client(region, client -> trying(() -> {
            String key = prefix + "/" + codeBundle.getName();
            log.info(format("Uploading: s3://%s/%s", bucket, key));

            String uploadId =
                    client.initiateMultipartUpload(new InitiateMultipartUploadRequest(bucket, key))
                          .getUploadId();
            return tuple(key, functionName, client.completeMultipartUpload(new CompleteMultipartUploadRequest(
                    bucket,
                    key,
                    uploadId,
                    map(createUploadPartRequest(bucket, codeBundle, key, uploadId))
                            .fmap(map(client::uploadPart))
                            .fmap(map(UploadPartResult::getPartETag))
                            .fmap(toCollection(ArrayList::new))
                            .apply(partNumbers(codeBundle.length())))));
        })
                .biMapL(Throwable::getMessage)));
    }

    private static Fn1<PartNumber, UploadPartRequest>
    createUploadPartRequest(String bucket, File codeBundle, String key, String uploadId) {
        return partNumber -> new UploadPartRequest()
                .withBucketName(bucket)
                .withKey(key)
                .withUploadId(uploadId)
                .withPartNumber(partNumber.getPartNumber())
                .withFileOffset(partNumber.getStartPosition())
                .withPartSize(partNumber.getPartSize())
                .withFile(codeBundle);
    }

    private static Fn1<Tuple3<String, Maybe<String>, CompleteMultipartUploadResult>, Either<String, Maybe<UpdateFunctionCodeResult>>>
    updateFunctionCode(String region, String bucket, boolean publish) {
        return t -> t.into((key, mFunctionName, uploadResult) -> mFunctionName
                .<UpdateFunctionCodeResult, Either<String, ?>, Maybe<UpdateFunctionCodeResult>, Either<String, Maybe<UpdateFunctionCodeResult>>>traverse(
                        functionName -> trying(() -> withLambdaClient(
                                region,
                                lambda -> lambda.updateFunctionCode(
                                        new UpdateFunctionCodeRequest()
                                                .withFunctionName(functionName)
                                                .withS3Bucket(bucket)
                                                .withS3Key(key)
                                                .withPublish(publish))))
                                .biMapL(Throwable::getMessage),
                        Either::right));
    }

    private static <T> T withLambdaClient(String region, Fn1<AWSLambda, T> fn) {
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

    private static <T> T withS3Client(String region, Fn1<AmazonS3, T> fn) {
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

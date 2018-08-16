package org.movealong.plugins.lambda;

import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugins.annotations.Mojo;

@Mojo(name = "deploy")
public class LambdaDeployMojo extends AbstractMojo {
    @Override
    public void execute() {
        getLog().info("Does nothing yet");
    }
}

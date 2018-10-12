/*
 * Copyright 2017 Google, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.netflix.spinnaker.clouddriver.controllers;

import com.netflix.spinnaker.clouddriver.artifacts.ArtifactCredentialsRepository;
import com.netflix.spinnaker.clouddriver.artifacts.ArtifactDownloader;
import com.netflix.spinnaker.clouddriver.artifacts.config.ArtifactCredentials;
import com.netflix.spinnaker.clouddriver.artifacts.helm.HelmArtifactCredentials;
import com.netflix.spinnaker.kork.artifacts.model.Artifact;
import com.netflix.spinnaker.kork.web.exceptions.NotFoundException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@Slf4j
@RestController
@RequestMapping("/artifacts")
public class ArtifactController {
  private ArtifactCredentialsRepository artifactCredentialsRepository;
  private ArtifactDownloader artifactDownloader;

  @Autowired
  public ArtifactController(Optional<ArtifactCredentialsRepository> artifactCredentialsRepository,
                            Optional<ArtifactDownloader> artifactDownloader) {
    this.artifactCredentialsRepository = artifactCredentialsRepository.orElse(null);
    this.artifactDownloader = artifactDownloader.orElse(null);
  }

  @RequestMapping(method = RequestMethod.GET, value = "/credentials")
  List<ArtifactCredentials> list() {
    if (artifactCredentialsRepository == null) {
      return new ArrayList<>();
    } else {
      return artifactCredentialsRepository.getAllCredentials();
    }
  }

  // PUT because we need to send a body, which GET does not allow for spring/retrofit
  @RequestMapping(method = RequestMethod.PUT, value = "/fetch")
  StreamingResponseBody fetch(@RequestBody Artifact artifact) {
    if (artifactDownloader == null) {
      throw new IllegalStateException("Artifacts have not been enabled. Enable them using 'artifacts.enabled' in clouddriver");
    }

    return outputStream -> IOUtils.copy(artifactDownloader.download(artifact), outputStream);
  }

  @RequestMapping(method = RequestMethod.GET, value = "/{type}/account/{accountName}/names")
  List<String> getNames(@PathVariable("type") String type,
                        @PathVariable("accountName") String accountName) {
    switch (type) {
      case "helm":
        return getHelmArtifactNames(accountName);
      default:
        throw new NotFoundException("Could not found artifact of type " + type);
    }
  }

  @RequestMapping(method = RequestMethod.GET, value = "/{type}/account/{accountName}/names/{artifactName}/versions")
  List<String> getVersions(@PathVariable("type") String type,
                           @PathVariable("accountName") String accountName,
                           @PathVariable("artifactName") String artifactName) {
    switch (type) {
      case "helm":
        return getHelmArtifactVersions(accountName, artifactName);
      default:
        throw new NotFoundException("Could not found artifact of type " + type);
    }
  }

  private List<String> getHelmArtifactNames(String accountName) {
    HelmArtifactCredentials helmArtifactCredentials = (HelmArtifactCredentials) findCredentials("helm/chart", accountName);
    InputStream index;
    List<String> names;
    try {
      index = helmArtifactCredentials.downloadIndex();
      names = helmArtifactCredentials.getIndexParser().findNames(index);
    } catch (IOException e) {
      throw new NotFoundException("Failed to download chart names for " + accountName + " account");
    }
    return names;
  }

  private List<String> getHelmArtifactVersions(String accountName, String artifactName) {
    HelmArtifactCredentials helmArtifactCredentials = (HelmArtifactCredentials) findCredentials("helm/chart", accountName);
    InputStream index;
    List<String> versions;
    try {
      index = helmArtifactCredentials.downloadIndex();
      versions = helmArtifactCredentials.getIndexParser().findVersions(index, artifactName);
    } catch (IOException e) {
      throw new NotFoundException("Failed to download chart versions for " + accountName + " account");
    }
    return versions;
  }


  private ArtifactCredentials findCredentials(String type, String name) {
    ArtifactCredentials artifactCredentials = artifactCredentialsRepository.getAllCredentials()
      .stream()
      .filter(e -> e.handlesType(type) && e.getName().equals(name))
      .findFirst()
      .orElseThrow(() -> new IllegalArgumentException("No credentials with name '" + name + "' could be found."));
    return artifactCredentials;
  }
}

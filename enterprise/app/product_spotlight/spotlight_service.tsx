interface SpotlightMetadata {
  title: string;
  description: string;
  category: string;
  date: string;
  image?: string;
  tags: string[];
  author: string;
}

interface Spotlight {
  id: string;
  metadata: SpotlightMetadata;
  content: string;
}

class SpotlightsService {
  private static instance: SpotlightsService;
  private spotlights: Spotlight[] = [];
  private loaded = false;

  static getInstance(): SpotlightsService {
    if (!SpotlightsService.instance) {
      SpotlightsService.instance = new SpotlightsService();
    }
    return SpotlightsService.instance;
  }

  async loadSpotlights(): Promise<Spotlight[]> {
    if (this.loaded) {
      return this.spotlights;
    }

    // In a production environment, this would make HTTP requests to load the markdown files
    // For now, we'll simulate loading from the file system
    const spotlightConfigs = [
      {
        id: "remote-execution-improvements",
        filename: "remote-execution-improvements.md"
      },
      {
        id: "buildbuddy-workflows-2.0", 
        filename: "buildbuddy-workflows-2.0.md"
      },
      {
        id: "code-search-beta",
        filename: "code-search-beta.md"
      },
      {
        id: "mastering-tests-dashboard",
        filename: "mastering-tests-dashboard.md"
      }
    ];

    const loadedSpotlights: Spotlight[] = [];

    for (const config of spotlightConfigs) {
      try {
        const spotlight = await this.loadSpotlightFile(config.id, config.filename);
        if (spotlight) {
          loadedSpotlights.push(spotlight);
        }
      } catch (error) {
        console.warn(`Failed to load spotlight ${config.id}:`, error);
      }
    }

    this.spotlights = loadedSpotlights;
    this.loaded = true;
    return this.spotlights;
  }

  async getSpotlight(id: string): Promise<Spotlight | null> {
    const spotlights = await this.loadSpotlights();
    return spotlights.find(s => s.id === id) || null;
  }

  async getAllSpotlights(): Promise<Spotlight[]> {
    return await this.loadSpotlights();
  }

  private async loadSpotlightFile(id: string, filename: string): Promise<Spotlight | null> {
    const response = await fetch(`/spotlights/${filename}`);
    if (!response.ok) {
      throw new Error(`Could not load spotlight file: ${filename}`);
    }
    const fileContent = await response.text();
    
    if (!fileContent) {
      return null;
    }

    const { metadata, content } = this.parseMarkdownFile(fileContent);
    
    return {
      id,
      metadata,
      content
    };
  }

  private parseMarkdownFile(fileContent: string): { metadata: SpotlightMetadata; content: string } {
    // Simple frontmatter parser
    const lines = fileContent.split('\n');
    let frontmatterEnd = -1;
    let frontmatterStart = -1;

    // Find frontmatter boundaries
    for (let i = 0; i < lines.length; i++) {
      if (lines[i].trim() === '---') {
        if (frontmatterStart === -1) {
          frontmatterStart = i;
        } else {
          frontmatterEnd = i;
          break;
        }
      }
    }

    if (frontmatterStart === -1 || frontmatterEnd === -1) {
      throw new Error('Invalid markdown file: missing frontmatter');
    }

    // Parse frontmatter (simple YAML-like parsing)
    const frontmatterLines = lines.slice(frontmatterStart + 1, frontmatterEnd);
    const metadata: any = {};
    
    for (const line of frontmatterLines) {
      const match = line.match(/^([^:]+):\s*(.+)$/);
      if (match) {
        const key = match[1].trim();
        let value: any = match[2].trim();
        
        // Handle arrays (tags)
        if (value.startsWith('[') && value.endsWith(']')) {
          value = value.slice(1, -1).split(',').map(s => s.trim().replace(/['"]/g, ''));
        }
        
        metadata[key] = value;
      }
    }

    // Get content after frontmatter
    const content = lines.slice(frontmatterEnd + 1).join('\n').trim();

    return { metadata: metadata as SpotlightMetadata, content };
  }
}

export default SpotlightsService;